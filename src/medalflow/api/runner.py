"""`run()` -- the single execution path from a project to a warehouse.

ADR 002, Decision 7. Discovery over all three layers, one cross-layer DAG,
the selector applied, and the surviving subgraph executed in topological
stages. There is never a per-layer runner.

Three decisions carry it.

**Compile first, and refuse to execute when compile reports errors** (D8). A
project that does not compile has no plan worth running, and executing the
part of it that did compile would build half a warehouse from a file the
author is still editing. The errors come back in the :class:`RunResult`
rather than as an exception: a caller asking "what happened" gets one answer
in one shape whether the project was broken or the warehouse was.

**Built on the seam that already existed, not around it.**
:meth:`~medalflow.medallion.types.ExecutionPlan.get_all_operations` with
``serialize=True`` is the plan->executor boundary the codebase was designed
around: it groups operations by stage and stamps ``_cte_stage``,
``_cte_position`` and ``_cte_request_context`` onto each dict for an external
orchestrator to fan out. `run()` consumes exactly that, one operation at a
time, through :func:`medalflow.api.execute` -- so "bring your own
orchestration" stays true, and `run()` is simply the one MedalFlow ships.

**Stop on the first failure.** A later stage exists because it depends on an
earlier one, so continuing past a failure would run operations whose inputs
were never built. Everything the run did not reach is reported as skipped
rather than silently dropped: succeeded, failed and skipped together are what
tell an author where they stand.
"""

from typing import Any

from medalflow.logging import get_logger
from medalflow.observability.context import execution_request_scope, resolve_request_context
from medalflow.types.base import CTEBaseModel

from .compiler import CompileResult, compile
from .platform import execute

logger = get_logger(__name__)


class PlannedOperation(CTEBaseModel):
    """Where one operation sits in the plan, and what it writes.

    Attributes:
        stage: The 1-based stage the operation belongs to. Stages run in
            order; this is the ``_cte_stage`` stamp.
        position: The operation's index within its stage -- the
            ``_cte_position`` stamp.
        schema_name: Schema the operation writes into.
        object_name: Object the operation builds.
        operation_type: What kind of operation it is, e.g. ``CREATE_TABLE``.
    """

    stage: int
    position: int
    schema_name: str
    object_name: str
    operation_type: str

    @classmethod
    def of(cls, operation: dict) -> "PlannedOperation":
        """Describe one serialized operation.

        Args:
            operation: An operation dict from ``get_all_operations(serialize=True)``

        Returns:
            The operation's place in the plan and its write target
        """
        return cls(
            stage=operation["_cte_stage"],
            position=operation["_cte_position"],
            schema_name=operation["schema_name"],
            object_name=operation["object_name"],
            operation_type=str(operation["operation_type"]),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize with every field present.

        Returns:
            The operation's identity in the plan
        """
        return {
            "stage": self.stage,
            "position": self.position,
            "schema_name": self.schema_name,
            "object_name": self.object_name,
            "operation_type": self.operation_type,
        }


class ExecutedOperation(PlannedOperation):
    """One operation the run reached, and what came back from it.

    ``success`` is read straight off
    :class:`~medalflow.compute.OperationResult`, which is where the platform
    already reports it. Deriving it from anything else has been wrong before:
    a successful ``CREATE TABLE`` returns ``rows_affected=None``, so a run
    inferring success from a row count would call every table it built a
    failure.

    Attributes:
        success: Whether the operation completed.
        duration_seconds: How long it took.
        error_type: The failure's category, when it failed.
        error_message: What the platform said, when it failed.
    """

    success: bool
    duration_seconds: float
    error_type: str | None = None
    error_message: str | None = None

    @classmethod
    def of_result(cls, operation: dict, result: Any) -> "ExecutedOperation":
        """Pair one operation with the result of executing it.

        Args:
            operation: The serialized operation that was executed
            result: The :class:`OperationResult` it produced

        Returns:
            The executed operation, as the run reports it
        """
        planned = PlannedOperation.of(operation)

        return cls(
            **planned.to_dict(),
            success=result.success,
            duration_seconds=result.duration_seconds,
            error_type=result.error_type,
            error_message=result.error_message,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize with every field present.

        Returns:
            The operation's identity plus its outcome
        """
        return {
            **super().to_dict(),
            "success": self.success,
            "duration_seconds": self.duration_seconds,
            "error_type": self.error_type,
            "error_message": self.error_message,
        }


class RunResult(CTEBaseModel):
    """What one `run()` did.

    It answers four questions in one shape: did it run at all, what executed,
    what failed and why, and what was consequently skipped. The compile it
    came from is carried whole, so a run that never started because the
    project would not compile reports the same structured errors
    :func:`~medalflow.api.compiler.compile` would have.

    Attributes:
        selector: The selector this run was asked for, as written.
        compile_result: The compile `run()` began with. Its ``errors`` are the
            reason nothing executed when they are non-empty.
        succeeded: Every operation that completed, in execution order.
        failed: The operation the run stopped on, or None.
        skipped: Everything the plan still held when the run stopped, in plan
            order -- including the rest of the failed operation's own stage.
    """

    selector: str
    compile_result: CompileResult
    succeeded: list[ExecutedOperation]
    failed: ExecutedOperation | None
    skipped: list[PlannedOperation]

    @property
    def ok(self) -> bool:
        """Whether the project compiled and everything planned then ran.

        Returns:
            True when there is nothing for the caller to fix
        """
        return self.compile_result.ok and self.failed is None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the whole result for a machine to read.

        ``ok`` is a property rather than a field, so it is carried explicitly
        or it would be missing from the very payload an agent loop reads.

        Returns:
            A JSON-serialisable dictionary
        """
        return {
            "selector": self.selector,
            "ok": self.ok,
            "compile": self.compile_result.to_dict(),
            "succeeded": [operation.to_dict() for operation in self.succeeded],
            "failed": self.failed.to_dict() if self.failed else None,
            "skipped": [operation.to_dict() for operation in self.skipped],
        }


def run(selector: str = "*") -> RunResult:
    """Compile a project and execute the plan it produced.

    Stages run in topological order. Operations within a stage are
    independent by construction, and run sequentially -- the stage boundary is
    the only ordering guarantee that matters, and nothing here needs more than
    that.

    Args:
        selector: Which models to run, in the v0.1 grammar -- ``*``,
            ``layer:bronze|silver|gold``, ``tag:<value>``, or a model's name.
            Defaults to everything.

    Returns:
        A :class:`RunResult`. A project that does not compile returns one with
        nothing executed and the compile errors intact; a selector matching no
        models returns one that ran nothing and is ``ok``.

    Raises:
        SelectorError: If the selector cannot be parsed, from `compile()`. The
            selector is the caller's input, so a typo surfaces immediately
            rather than reading as "nothing matched".
    """
    compiled = compile(selector)

    if not compiled.ok:
        # Every operation the partial compile did produce is reported skipped:
        # "nothing ran" is the answer, and naming what did not run is more use
        # than an empty result.
        logger.warning(
            "run.refused",
            extra={"selector": selector, "error_count": len(compiled.errors)},
        )

        return RunResult(
            selector=selector,
            compile_result=compiled,
            succeeded=[],
            failed=None,
            skipped=_planned(_operations(compiled)),
        )

    ctx = resolve_request_context(None)
    compiled.plan.attach_context(ctx)

    with execution_request_scope(ctx, operation="medalflow.api.run"):
        succeeded, failed, remaining = _execute(_operations(compiled), ctx)

    logger.info(
        "run.complete",
        extra={
            "selector": selector,
            "succeeded_count": len(succeeded),
            "failed": failed.object_name if failed else None,
            "skipped_count": len(remaining),
        },
    )

    return RunResult(
        selector=selector,
        compile_result=compiled,
        succeeded=succeeded,
        failed=failed,
        skipped=_planned(remaining),
    )


# --- the pieces ------------------------------------------------------------


def _operations(compiled: CompileResult) -> list[dict]:
    """Flatten the plan's stages into the order they must execute in.

    The stamps are what make flattening safe: each dict still carries the
    ``_cte_stage`` it came from, so the stage structure survives the list.

    Args:
        compiled: The compile whose plan is to be run

    Returns:
        Every serialized operation, stage by stage
    """
    staged = compiled.plan.get_all_operations(serialize=True)

    return [operation for stage in staged for operation in stage]


def _execute(
    operations: list[dict], ctx: Any
) -> tuple[list[ExecutedOperation], ExecutedOperation | None, list[dict]]:
    """Execute operations in order, stopping at the first failure.

    Nothing here catches exceptions. The platform already turns every
    operation-level failure into an unsuccessful ``OperationResult``; what is
    left that can raise -- an unbuildable platform, an operation dict MedalFlow
    itself malformed -- is not something the author can act on and does not
    belong in a run report.

    Args:
        operations: The serialized operations, in execution order
        ctx: The request context to execute under

    Returns:
        What succeeded, what failed, and what was never reached
    """
    succeeded: list[ExecutedOperation] = []

    for index, operation in enumerate(operations):
        executed = ExecutedOperation.of_result(operation, execute(operation, ctx=ctx))

        if not executed.success:
            return succeeded, executed, operations[index + 1 :]

        succeeded.append(executed)

    return succeeded, None, []


def _planned(operations: list[dict]) -> list[PlannedOperation]:
    """Describe operations that were not executed.

    Args:
        operations: Serialized operations the run did not reach

    Returns:
        Their identities, in plan order
    """
    return [PlannedOperation.of(operation) for operation in operations]


__all__ = ["ExecutedOperation", "PlannedOperation", "RunResult", "run"]
