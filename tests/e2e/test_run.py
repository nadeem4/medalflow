"""`run(selector)` -- the single execution path (ADR 002, Decision 7).

Discovery over all layers, one cross-layer DAG, the selector applied, and the
subgraph executed in topological stages. There is never a per-layer runner.

`run()` is built on the seam that already existed:
`ExecutionPlan.get_all_operations(serialize=True)` stamps `_cte_stage` and
`_cte_position` onto each operation dict, and `medalflow.api.execute` consumes
one such dict. Nothing here replaces that seam, so the promise that
orchestration can stay with an existing tool survives.

The executor is faked -- there is no warehouse in this suite (D6) -- but the
plan, the selector and the stage order are all real.
"""

import json

import pytest
from medalflow.api.runner import RunResult, run
from medalflow.compute import OperationResult


class _Executor:
    """A stand-in for `medalflow.api.execute`, recording what it was handed.

    Every operation succeeds unless the test names one that must not.
    """

    def __init__(self, failing: str | None = None):
        self.failing = failing
        self.calls: list[dict] = []

    def __call__(self, operation, *args, **kwargs):
        self.calls.append(operation)
        object_name = operation["object_name"]

        return OperationResult(
            success=object_name != self.failing,
            operation_type=operation["operation_type"],
            schema_name=operation["schema_name"],
            object_name=object_name,
            duration_seconds=0.5,
            error_message="table does not exist" if object_name == self.failing else None,
            error_type="ProgrammingError" if object_name == self.failing else None,
        )

    @property
    def object_names(self) -> list[str]:
        return [call["object_name"] for call in self.calls]


@pytest.fixture
def executor(monkeypatch):
    """Fake the one function `run()` executes through.

    `medalflow.api.execute` builds a platform for itself, so the alternative
    to patching it is a constructor argument on `run()` that exists only for
    the tests. Patching is the smaller of the two.
    """

    def _install(failing: str | None = None) -> _Executor:
        from medalflow.api import runner

        recorder = _Executor(failing)
        monkeypatch.setattr(runner, "execute", recorder)
        return recorder

    return _install


# --- every stage, in order -------------------------------------------------


def test_run_executes_every_operation_in_topological_order(sample_project, executor):
    """The stage order is the whole correctness claim: silver reads the bronze
    tables, gold reads the silver ones."""
    recorder = executor()

    run("*")

    assert recorder.object_names == [
        "Customers",
        "Orders",
        "DimCustomer",
        "FactOrders",
        "vw_Revenue",
    ]


def test_a_successful_run_is_ok(sample_project, executor):
    executor()

    result = run("*")

    assert result.ok is True
    assert [outcome.object_name for outcome in result.succeeded] == [
        "Customers",
        "Orders",
        "DimCustomer",
        "FactOrders",
        "vw_Revenue",
    ]
    assert result.failed is None
    assert result.skipped == []


def test_run_returns_a_run_result(sample_project, executor):
    executor()

    assert isinstance(run("*"), RunResult)


# --- the selector narrows what runs ----------------------------------------


def test_a_selector_narrows_what_is_executed(sample_project, executor):
    recorder = executor()

    result = run("layer:bronze")

    assert recorder.object_names == ["Customers", "Orders"]
    assert result.selector == "layer:bronze"
    assert result.ok is True


def test_a_selector_matching_nothing_runs_nothing_and_is_still_ok(sample_project, executor):
    """Narrowing to something a project does not declare is a real answer, and
    the same one `compile()` gives: an empty plan, not a failure."""
    recorder = executor()

    result = run("no_such_model")

    assert recorder.calls == []
    assert result.succeeded == []
    assert result.skipped == []
    assert result.ok is True


# --- compile errors stop the run before it starts --------------------------


def test_compile_errors_prevent_execution_entirely(broken_project, executor):
    """D8: `run()` refuses to execute when compile reports errors. Not one
    operation runs -- not even the model that compiled fine, because a project
    the author is mid-edit on is not a project to build a warehouse from."""
    recorder = executor()

    result = run("*")

    assert recorder.calls == []
    assert result.succeeded == []
    assert result.failed is None
    assert result.ok is False


def test_a_refused_run_carries_the_compile_errors(broken_project, executor):
    executor()

    result = run("*")

    assert sorted(error.model for error in result.compile_result.errors) == [
        "NoTable",
        "NotSql",
        "Raises",
    ]


def test_a_refused_run_names_what_it_did_not_run(broken_project, executor):
    """One model in `broken_project` compiles. Saying so is more use than an
    empty result: it is the difference between 'nothing to do' and 'this was
    ready and did not run'."""
    executor()

    result = run("*")

    assert [operation.object_name for operation in result.skipped] == ["Good"]


# --- a failure mid-plan ----------------------------------------------------


def test_a_failure_stops_the_run(sample_project, executor):
    """Silver reads the bronze tables and gold reads the silver ones, so
    continuing past a failure would run operations whose inputs do not exist."""
    recorder = executor(failing="DimCustomer")

    run("*")

    assert recorder.object_names == ["Customers", "Orders", "DimCustomer"]


def test_a_failure_is_reported_with_what_succeeded_and_what_was_skipped(sample_project, executor):
    executor(failing="DimCustomer")

    result = run("*")

    assert [operation.object_name for operation in result.succeeded] == [
        "Customers",
        "Orders",
    ]
    assert result.failed.object_name == "DimCustomer"
    assert [operation.object_name for operation in result.skipped] == [
        "FactOrders",
        "vw_Revenue",
    ]
    assert result.ok is False


def test_the_failure_says_why(sample_project, executor):
    executor(failing="DimCustomer")

    failed = run("*").failed

    assert failed.success is False
    assert failed.error_type == "ProgrammingError"
    assert failed.error_message == "table does not exist"


def test_success_is_read_off_the_operation_result(sample_project, executor):
    """A successful CREATE TABLE reports `rows_affected=None`, so a run that
    inferred success from a row count would call every table it built a
    failure. `OperationResult.success` is where the platform already says it."""
    executor()

    result = run("layer:bronze")

    assert all(operation.success for operation in result.succeeded)
    assert result.succeeded[0].duration_seconds == 0.5


# --- the seam's contract ---------------------------------------------------


def test_operations_reach_the_executor_with_their_stage_stamps(sample_project, executor):
    """`get_all_operations(serialize=True)` is the plan -> executor seam, and
    the stamps are what it exists to add. `run()` consumes that seam rather
    than replacing it, so they have to survive the trip."""
    recorder = executor()

    run("*")

    assert [(call["_cte_stage"], call["_cte_position"]) for call in recorder.calls] == [
        (1, 0),
        (1, 1),
        (2, 0),
        (3, 0),
        (4, 0),
    ]


def test_the_reported_stages_are_the_stamped_ones(sample_project, executor):
    executor()

    result = run("*")

    assert [(operation.stage, operation.position) for operation in result.succeeded] == [
        (1, 0),
        (1, 1),
        (2, 0),
        (3, 0),
        (4, 0),
    ]


# --- machine-readable ------------------------------------------------------


def test_a_run_result_survives_a_json_round_trip(sample_project, executor):
    executor(failing="DimCustomer")

    restored = json.loads(json.dumps(run("*").to_dict()))

    assert restored["ok"] is False
    assert restored["selector"] == "*"
    assert restored["compile"]["ok"] is True
    assert [operation["object_name"] for operation in restored["succeeded"]] == [
        "Customers",
        "Orders",
    ]
    assert restored["failed"]["error_type"] == "ProgrammingError"
    assert [operation["object_name"] for operation in restored["skipped"]] == [
        "FactOrders",
        "vw_Revenue",
    ]


def test_a_refused_run_survives_a_json_round_trip(broken_project, executor):
    executor()

    restored = json.loads(json.dumps(run("*").to_dict()))

    assert restored["ok"] is False
    assert restored["succeeded"] == []
    assert restored["failed"] is None
    assert len(restored["compile"]["errors"]) == 3


# --- the public surface ----------------------------------------------------


def test_run_is_exported_from_the_api_and_the_package():
    import medalflow
    import medalflow.api

    assert medalflow.api.run is run
    assert medalflow.run is run
    assert medalflow.RunResult is RunResult


# --- through the real executor ---------------------------------------------


def test_run_executes_through_the_shipped_execute_seam(sample_project, monkeypatch):
    """Every other test here fakes `runner.execute`, which proves the loop but
    not what the loop calls. This one fakes the *platform* instead, so the
    operation dict travels the whole real path -- `api.execute`, its
    instrumentation, `platform.execute` -- exactly as it would in a deployment.
    """
    from medalflow.api import platform as platform_module

    received: list[dict] = []

    class _Platform:
        def execute(self, operation, telemetry=None):
            received.append(operation)

            return OperationResult(
                success=True,
                operation_type=operation["operation_type"],
                schema_name=operation["schema_name"],
                object_name=operation["object_name"],
                duration_seconds=0.0,
            )

    monkeypatch.setattr(platform_module, "create_platform", lambda environment: _Platform())

    result = run("layer:bronze")

    assert [operation["object_name"] for operation in received] == ["Customers", "Orders"]
    assert result.ok is True
    assert [operation.stage for operation in result.succeeded] == [1, 1]


def test_there_is_no_per_layer_entry_point():
    """ADR 002 D7: 'There is never a per-layer runner.'

    `compile("layer:bronze").plan` is what the four per-layer plan functions
    used to be, and bronze models are named per table now, so
    `compile("Customers")` covers the table-selection case they also served.
    """
    import medalflow
    import medalflow.api

    for name in (
        "get_bronze_execution_plan",
        "get_gold_execution_plan",
        "get_silver_execution_plan_for_models",
        "get_execution_plan_for_sps",
    ):
        assert not hasattr(medalflow, name)
        assert not hasattr(medalflow.api, name)


def test_the_orchestrator_has_no_per_layer_plan_method():
    """The four functions were their only callers. `create_plan_from_sequencers`
    and `create_execution_plan` have others and stay."""
    from medalflow.medallion import ExecutionPlanOrchestrator

    for name in (
        "create_plan_for_bronze_layer",
        "create_plan_for_gold_layer",
        "create_plan_for_silver_layer",
    ):
        assert not hasattr(ExecutionPlanOrchestrator, name)

    assert hasattr(ExecutionPlanOrchestrator, "create_plan_from_sequencers")
    assert hasattr(ExecutionPlanOrchestrator, "create_execution_plan")


def test_the_request_context_reaches_the_executor_too(sample_project, executor):
    """The third stamp `get_all_operations` adds. It is empty unless a context
    was attached to the plan, so `run()` attaches one -- otherwise every
    operation it executes is untraceable back to the run that issued it."""
    recorder = executor()

    run("layer:bronze")

    request_ids = {call["_cte_request_context"]["request_id"] for call in recorder.calls}

    assert len(request_ids) == 1
    assert request_ids != {None}
