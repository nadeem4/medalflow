"""`compile()` -- the public step between authoring a project and running it.

ADR 002, Decision 8. `compile()` walks every layer's configured package,
narrows what it found with a selector, builds one cross-layer execution plan,
and hands back a :class:`CompileResult`: the models it compiled, the plan, and
a list of structured errors.

Two properties carry the decision, and both are about the errors.

**Machine-readable first.** A :class:`CompileError` is five fields --
``file``, ``model``, ``error_type``, ``message``, ``suggestion`` -- and the
whole result is JSON-serialisable end to end. Human text is rendered *from*
that, never the other way round: this is the loop a coding agent iterates on.

**Collected, not raised.** A model whose SQL method raises, one whose
`@query_metadata` names no table, a layer with no package configured -- each
becomes an entry in ``errors`` and compile keeps going. An author fixing three
models learns about all three from one run, and the models that do work still
reach the plan.

Where the line is drawn: everything that is a *project* problem is collected.
A caller problem is not -- an unparseable selector raises
:class:`~medalflow.api.selectors.SelectorError` before any discovery runs,
because returning an empty result would make a typo indistinguishable from a
project that declares no matching model. Genuine bugs inside MedalFlow itself
still propagate; they are not something the author can act on.

Compiling is offline (D6). Bronze models are declared, so nothing here needs a
warehouse or a credential.
"""

import inspect
from typing import Any

from pydantic import Field

from medalflow.logging import get_logger
from medalflow.medallion.bronze.metadata_discovery import BronzeMetadataDiscovery
from medalflow.medallion.gold.metadata_discovery import GoldMetadataDiscovery
from medalflow.medallion.orchestration.execution_orchestrator import ExecutionPlanOrchestrator
from medalflow.medallion.silver.metadata_discovery import SilverMetadataDiscovery
from medalflow.medallion.types import ExecutionPlan
from medalflow.settings import get_settings
from medalflow.settings.main import MODEL_LAYERS
from medalflow.types.base import CTEBaseModel
from medalflow.types.metadata import _shadowing_schema_is_intended

from .selectors import parse_selector

logger = get_logger(__name__)

# One discovery per layer. They are thin subclasses of the same package walk,
# so compile treats them identically.
LAYER_DISCOVERIES = {
    "bronze": BronzeMetadataDiscovery,
    "silver": SilverMetadataDiscovery,
    "gold": GoldMetadataDiscovery,
}

# The plan's `sequencer_name`. A compile spans every sequencer it found, so no
# single one of them names the plan.
PLAN_NAME = "compile"


class CompileError(CTEBaseModel):
    """One thing wrong with a project, in the shape ADR 002 D8 decided.

    The five fields are the contract. Anything rendered for a human is
    rendered from them.

    Attributes:
        file: Source file the problem is in, so an editor can jump to it.
            None when the problem is not about a file -- an unconfigured
            layer package, for instance.
        model: The declared `name` of the model at fault, or None when the
            problem is the layer rather than any one model.
        error_type: Short machine-readable category. The underlying
            exception's class name where there is one.
        message: What went wrong.
        suggestion: What to do about it.
    """

    file: str | None = None
    model: str | None = None
    error_type: str
    message: str
    suggestion: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize with every field present.

        Overridden because the inherited ``to_dict`` drops None fields, and a
        consumer parsing these needs one stable shape rather than a key that
        appears only when a model happens to be named.

        Returns:
            The five fields, Nones included
        """
        return {
            "file": self.file,
            "model": self.model,
            "error_type": self.error_type,
            "message": self.message,
            "suggestion": self.suggestion,
        }

    def __str__(self) -> str:
        """Render the structured fields as one line of human text.

        Returns:
            Something an author can read, built from the fields and nothing else
        """
        where = " ".join(
            part for part in (self.file, f"({self.model})" if self.model else "") if part
        )
        line = f"{self.error_type}: {self.message}"

        if where:
            line = f"{where} -- {line}"
        if self.suggestion:
            line = f"{line} {self.suggestion}"

        return line


with _shadowing_schema_is_intended():

    class CompiledModel(CTEBaseModel):
        """One model `compile()` found and kept.

        Attributes:
            name: The model's declared identity -- what discovery keys on and
                what a selector matches.
            layer: 'bronze', 'silver' or 'gold'. The medallion layer the model
                was discovered in, which is not necessarily
                ``GoldMetadata.layer``: that one is a free-text label an author
                may set to 'gold_ml', and a selector has to keep working.
            schema: The schema the model declares it writes into.
            description: The model's declared description, or "".
            tags: The model's declared tags.
        """

        name: str
        layer: str
        schema: str
        description: str = ""
        tags: list[str] = Field(default_factory=list)

        def to_dict(self) -> dict[str, Any]:
            """Serialize with every field present.

            Returns:
                The five declared fields
            """
            return {
                "name": self.name,
                "layer": self.layer,
                "schema": self.schema,
                "description": self.description,
                "tags": list(self.tags),
            }


class CompileResult(CTEBaseModel):
    """What one `compile()` produced.

    Attributes:
        selector: The selector this compile was asked for, as written.
        models: The models the selector kept. A model appears here because
            discovery found it, whether or not building its operations
            succeeded -- "discovery found it, planning it failed" is the useful
            distinction, and collapsing the two would hide it.
        plan: The cross-layer execution plan for the selected models. Always a
            plan: a selector matching nothing compiles to an empty one, which
            is a real answer rather than a failure.
        errors: Everything wrong with the project, collected.
    """

    selector: str
    models: list[CompiledModel]
    plan: ExecutionPlan
    errors: list[CompileError]

    @property
    def ok(self) -> bool:
        """Whether the project compiled cleanly.

        Returns:
            True when nothing went wrong. `run()` refuses to execute otherwise.
        """
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        """Serialize the whole result for a machine to read.

        Overridden rather than inherited so the nested plan goes through its
        own ``to_dict`` and ``ok`` is carried explicitly -- a property is not a
        field, so it would otherwise be missing from the very payload the
        agent loop reads.

        Returns:
            A JSON-serialisable dictionary
        """
        return {
            "selector": self.selector,
            "ok": self.ok,
            "models": [model.to_dict() for model in self.models],
            "plan": self.plan.to_dict(),
            "errors": [error.to_dict() for error in self.errors],
        }


def compile(selector: str = "*") -> CompileResult:
    """Compile a project's models into one cross-layer execution plan.

    Every layer is discovered, the selector narrows what was found, and the
    surviving models' operations become a single staged plan with the
    bronze -> silver -> gold edges the models' own SQL implies.

    Errors are collected, not raised: the result carries one
    :class:`CompileError` per problem so an author sees all of them at once.

    Args:
        selector: Which models to compile, in the v0.1 grammar -- ``*``,
            ``layer:bronze|silver|gold``, ``tag:<value>``, or a model's name.
            Defaults to everything.

    Returns:
        A :class:`CompileResult`. A selector that matches no models is *not*
        an error: it compiles to an empty plan, which is what someone
        narrowing to ``layer:gold`` in a project with no gold models is
        asking for.

    Raises:
        SelectorError: If the selector cannot be parsed. The selector is the
            caller's input rather than the project's content, so a typo
            surfaces immediately instead of reading as "nothing matched".
    """
    parsed = parse_selector(selector)
    settings = get_settings()

    models: list[CompiledModel] = []
    errors: list[CompileError] = []
    operations: list[Any] = []

    for layer in MODEL_LAYERS:
        discovered, layer_errors = _discover(layer, settings)
        errors.extend(layer_errors)

        for record in discovered:
            model = _compiled_model(record, layer)

            if not parsed.matches(model):
                continue

            models.append(model)

            try:
                operations.extend(record.sequencer_class(settings).get_queries())
            except Exception as e:
                errors.append(_model_error(record, model, e))

    plan, plan_errors = _plan(operations, settings)
    errors.extend(plan_errors)

    logger.info(
        "compile.complete",
        extra={
            "selector": selector,
            "model_count": len(models),
            "error_count": len(errors),
        },
    )

    return CompileResult(selector=selector, models=models, plan=plan, errors=errors)


# --- the pieces ------------------------------------------------------------


def _discover(layer: str, settings) -> tuple[list[Any], list[CompileError]]:
    """Discover one layer's models, turning its failures into errors.

    Two things can go wrong before a single model is read, and neither is a
    reason to abandon the other layers: the layer may have no package
    configured, and the walk itself may fail (a module that will not import,
    two models sharing a name).

    Args:
        layer: 'bronze', 'silver' or 'gold'
        settings: Application settings

    Returns:
        The layer's discovered metadata records, and any errors that stopped
        it from producing them
    """
    try:
        package = settings.package_for_layer(layer)
    except ValueError as e:
        return [], [
            CompileError(
                model=None,
                error_type="UnconfiguredPackage",
                message=str(e),
                suggestion=(
                    f"Set MEDALFLOW_{layer.upper()}_PACKAGE to the package holding "
                    f"your {layer} models, or MEDALFLOW_MODELS_PACKAGE to the package "
                    f"whose '{layer}' subpackage holds them. A project with no {layer} "
                    f"models can leave this unset once the other layers are configured."
                ),
            )
        ]

    discovery = LAYER_DISCOVERIES[layer](package, settings=settings)

    try:
        # Forced: compile is what an author runs after editing their models,
        # so reading a cached walk would report the project as it used to be.
        return discovery.discover_all(force_refresh=True), []
    except Exception as e:
        return [], [
            CompileError(
                model=None,
                error_type=type(e).__name__,
                message=f"Could not discover the {layer} models in '{package}': {e}",
                suggestion=(
                    f"Check that every module under '{package}' imports, and that no "
                    f"two {layer} models declare the same name."
                ),
            )
        ]


def _compiled_model(record: Any, layer: str) -> CompiledModel:
    """Describe one discovered model in the terms a selector matches on.

    Args:
        record: The layer's discovery metadata record
        layer: The medallion layer it was discovered in

    Returns:
        The model, as the result reports it
    """
    declaration = getattr(record, f"{layer}_metadata")

    return CompiledModel(
        name=record.name,
        layer=layer,
        schema=declaration.schema,
        description=record.description,
        tags=record.tags,
    )


def _model_error(record: Any, model: CompiledModel, error: Exception) -> CompileError:
    """Turn one model's failure into an error the author can act on.

    Args:
        record: The layer's discovery metadata record
        model: The compiled description of the model
        error: What building its operations raised

    Returns:
        The structured error
    """
    return CompileError(
        file=_source_file(record.sequencer_class),
        model=model.name,
        error_type=type(error).__name__,
        message=str(error),
        suggestion=(
            f"Fix {model.name} in the file above. Every @query_metadata method must "
            f"name the table it writes, be callable with no arguments at compile "
            f"time, and return a SQL string."
        ),
    )


def _source_file(cls: type) -> str | None:
    """The file a model class is declared in, when it can be found.

    Args:
        cls: The decorated model class

    Returns:
        Its source file, or None for a class with no file behind it (one
        defined in a REPL, or in an extension module)
    """
    try:
        return inspect.getfile(cls)
    except (OSError, TypeError):
        return None


def _plan(operations: list[Any], settings) -> tuple[ExecutionPlan, list[CompileError]]:
    """Build the cross-layer plan, turning its failure into an error too.

    Args:
        operations: Every operation the selected models produced
        settings: Application settings

    Returns:
        The plan and any error that stopped a real one being built. An empty
        selection yields an empty plan and no error.
    """
    if not operations:
        return _empty_plan(), []

    try:
        return (
            ExecutionPlanOrchestrator(settings).create_execution_plan(
                operations=operations, sequencer_name=PLAN_NAME
            ),
            [],
        )
    except Exception as e:
        return _empty_plan(), [
            CompileError(
                model=None,
                error_type=type(e).__name__,
                message=f"Could not build an execution plan: {e}",
                suggestion=(
                    "The models compiled, but their dependencies do not form a DAG. "
                    "Look for two models whose SQL reads from each other."
                ),
            )
        ]


def _empty_plan() -> ExecutionPlan:
    """A plan with nothing in it.

    Returns:
        An ExecutionPlan of no stages, so `CompileResult.plan` is always a
        plan and a caller never has to test it for None
    """
    return ExecutionPlan(
        sequencer_name=PLAN_NAME,
        metadata=None,
        lineage=None,
        total_queries=0,
        stages=[],
        dependency_graph={},
    )


__all__ = ["CompileError", "CompileResult", "CompiledModel", "compile"]
