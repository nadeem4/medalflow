"""`compile()` as a public step (ADR 002, Decision 8).

`compile()` walks every layer, applies a selector, builds one cross-layer plan
and returns a `CompileResult`: the models it compiled, the plan, and a list of
structured errors. Everything here runs against real projects under
tests/fixtures, entirely offline (D6) -- bronze models are declared, so no
warehouse and no credentials are involved.

The decision the whole file is about: **compile collects errors, it does not
raise them.** An author with three broken models learns about all three from
one run, and the models that do work still reach the plan.
"""

import json
import sys
from pathlib import Path

import pytest
from medalflow.api.compiler import CompileError, CompileResult, compile
from medalflow.api.selectors import SelectorError

from tests.conftest import OFFLINE_ENV

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"

FIXTURE_PACKAGES = ("sample_project", "broken_project")


@pytest.fixture
def project(monkeypatch):
    """Point real settings at a fixture project, offline.

    Yields a callable taking the settings overrides a project needs, so each
    test states its own configuration -- which is what makes 'this layer has
    no package configured' testable at all.
    """
    from medalflow.settings import main as settings_main

    monkeypatch.syspath_prepend(str(FIXTURES))

    def _configure(**overrides):
        for name in [m for m in sys.modules if m.split(".")[0] in FIXTURE_PACKAGES]:
            del sys.modules[name]

        for key, value in {**OFFLINE_ENV, **overrides}.items():
            monkeypatch.setenv(key, value)

        return settings_main.get_settings(force_reload=True)

    try:
        yield _configure
    finally:
        # The singleton outlives monkeypatch's env cleanup.
        settings_main._settings = None


@pytest.fixture
def sample_project(project):
    """The five-model project: two bronze, two silver, one gold."""
    return project(
        MEDALFLOW_MODELS_PACKAGE="sample_project",
        MEDALFLOW_CONFIGURED_MODELS="sales",
    )


@pytest.fixture
def broken_project(project):
    """Three silver models broken three different ways, and one that works."""
    return project(
        MEDALFLOW_MODELS_PACKAGE="broken_project",
        MEDALFLOW_CONFIGURED_MODELS="sales",
    )


def _names(result):
    return sorted(model.name for model in result.models)


# --- a healthy project -----------------------------------------------------


def test_compile_finds_every_model_in_every_layer(sample_project):
    result = compile("*")

    assert _names(result) == [
        "Customers",
        "Orders",
        "Revenue",
        "usp_load_dim_customer",
        "usp_load_fact_orders",
    ]
    assert {model.name: model.layer for model in result.models} == {
        "Customers": "bronze",
        "Orders": "bronze",
        "usp_load_dim_customer": "silver",
        "usp_load_fact_orders": "silver",
        "Revenue": "gold",
    }


def test_compile_reports_what_each_model_declares(sample_project):
    revenue = next(model for model in compile("*").models if model.name == "Revenue")

    assert revenue.schema == "gold"
    assert revenue.description == "Revenue reporting view"
    assert revenue.tags == ["daily"]


def test_compile_plans_all_five_operations_across_the_layers(sample_project):
    result = compile("*")

    assert result.plan.total_queries == 5
    staged = [
        sorted(operation.object_name for operation in stage.operations)
        for stage in result.plan.stages
    ]
    assert staged == [["Customers", "Orders"], ["DimCustomer"], ["FactOrders"], ["vw_Revenue"]]


def test_a_healthy_project_compiles_without_errors(sample_project):
    result = compile("*")

    assert result.errors == []
    assert result.ok is True


# --- the selector narrows what is compiled ---------------------------------


@pytest.mark.parametrize(
    ("selector", "expected"),
    [
        ("layer:bronze", ["Customers", "Orders"]),
        ("layer:silver", ["usp_load_dim_customer", "usp_load_fact_orders"]),
        ("layer:gold", ["Revenue"]),
    ],
)
def test_a_layer_selector_compiles_only_that_layer(sample_project, selector, expected):
    result = compile(selector)

    assert _names(result) == expected
    assert result.plan.total_queries == len(expected)


def test_a_tag_selector_crosses_layers(sample_project):
    """`daily` is declared by one silver model and by the gold model."""
    assert _names(compile("tag:daily")) == ["Revenue", "usp_load_dim_customer"]


def test_a_name_selector_compiles_one_model(sample_project):
    result = compile("usp_load_fact_orders")

    assert _names(result) == ["usp_load_fact_orders"]
    assert result.plan.total_queries == 1


def test_a_selector_matching_nothing_is_an_empty_plan_not_an_error(sample_project):
    """Narrowing to something a project does not declare is a real answer."""
    result = compile("no_such_model")

    assert result.models == []
    assert result.plan.total_queries == 0
    assert result.plan.stages == []
    assert result.ok is True


# --- selectors the caller got wrong ----------------------------------------


@pytest.mark.parametrize("selector", ["+Revenue", "Revenue+"])
def test_the_v0_3_graph_operators_are_refused_by_name(sample_project, selector):
    with pytest.raises(SelectorError, match="not yet supported"):
        compile(selector)


def test_an_unparseable_selector_reaches_the_caller_immediately(sample_project):
    """A typo must never read as 'nothing matched'."""
    with pytest.raises(SelectorError, match="owner"):
        compile("owner:nadeem")


# --- an unconfigured layer is an error, not a crash ------------------------


def test_an_unconfigured_layer_package_becomes_an_error(project):
    """A project with no gold models still compiles its bronze and silver."""
    project(
        MEDALFLOW_BRONZE_PACKAGE="sample_project.bronze",
        MEDALFLOW_SILVER_PACKAGE="sample_project.silver",
        MEDALFLOW_CONFIGURED_MODELS="sales",
    )

    result = compile("*")

    assert _names(result) == [
        "Customers",
        "Orders",
        "usp_load_dim_customer",
        "usp_load_fact_orders",
    ]
    assert result.plan.total_queries == 4

    (error,) = result.errors
    assert error.model is None
    assert "gold" in error.message
    assert "MEDALFLOW_GOLD_PACKAGE" in error.suggestion
    assert result.ok is False


# --- several broken models, one run ----------------------------------------


def test_every_broken_model_is_reported_in_one_run(broken_project):
    """Three models are broken three different ways. Compile finds all three."""
    result = compile("*")

    assert sorted(error.model for error in result.errors) == [
        "usp_load_no_table",
        "usp_load_not_sql",
        "usp_load_raises",
    ]
    assert result.ok is False


def test_the_models_that_work_still_reach_the_plan(broken_project):
    """A broken sibling does not quietly shrink the plan to nothing."""
    result = compile("*")

    assert result.plan.total_queries == 1
    assert [
        operation.object_name for stage in result.plan.stages for operation in stage.operations
    ] == ["Good"]


def test_a_broken_model_is_still_reported_as_discovered(broken_project):
    """Discovery found it; building its operations is what failed."""
    assert "usp_load_raises" in _names(compile("*"))


def test_a_compile_error_names_the_file_it_is_in(broken_project):
    error = next(error for error in compile("*").errors if error.model == "usp_load_raises")

    assert Path(error.file).name == "models.py"
    assert Path(error.file).parent.name == "silver"


def test_a_compile_error_carries_the_underlying_failure(broken_project):
    error = next(error for error in compile("*").errors if error.model == "usp_load_raises")

    assert "this model" in error.message
    assert error.error_type


# --- structured first, human text rendered from it -------------------------


def test_human_text_is_rendered_from_the_structured_fields():
    error = CompileError(
        file="models.py",
        model="usp_load_raises",
        error_type="ValueError",
        message="the model failed",
        suggestion="Fix the method it names.",
    )

    rendered = str(error)

    assert "models.py" in rendered
    assert "usp_load_raises" in rendered
    assert "the model failed" in rendered
    assert "Fix the method it names." in rendered


def test_a_compile_result_survives_a_json_round_trip(broken_project):
    """Phase 8's agent loop reads this; it has to be JSON end to end."""
    result = compile("*")

    restored = json.loads(json.dumps(result.to_dict()))

    assert restored["ok"] is False
    assert restored["selector"] == "*"
    assert sorted(error["model"] for error in restored["errors"]) == [
        "usp_load_no_table",
        "usp_load_not_sql",
        "usp_load_raises",
    ]
    assert restored["plan"]["total_queries"] == 1
    assert {model["name"] for model in restored["models"]} >= {"usp_load_good"}


def test_a_healthy_compile_result_survives_a_json_round_trip(sample_project):
    restored = json.loads(json.dumps(compile("*").to_dict()))

    assert restored["ok"] is True
    assert restored["errors"] == []
    assert len(restored["plan"]["stages"]) == 4


def test_a_compile_error_has_exactly_the_decided_fields():
    """ADR 002 D8 names the shape: {file, model, error_type, message, suggestion}."""
    assert set(CompileError.model_fields) == {
        "file",
        "model",
        "error_type",
        "message",
        "suggestion",
    }


# --- the public surface ----------------------------------------------------


def test_compile_is_exported_from_the_api_and_the_package():
    import medalflow
    import medalflow.api

    assert medalflow.api.compile is compile
    assert medalflow.compile is compile
    assert medalflow.CompileResult is CompileResult
    assert medalflow.CompileError is CompileError
