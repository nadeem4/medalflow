"""How bronze reaches its models (ADR 002, Decisions 6 and 7).

A declared bronze model is the default and introspection is the documented
alternative. The mode is meant to be chosen on configuration --
`bronze_introspection` -- and never on whether models happen to be found:
inferring it would mean a typo in `MEDALFLOW_BRONZE_PACKAGE` silently falls
back to hitting a warehouse, which is the offline-compile guarantee (D6)
failing quietly instead of loudly.

There is no per-layer runner any more (D7), so the plan below is
`compile("layer:bronze").plan`. `compile()` reads the declared models; the
introspecting sequencer is exercised directly in `test_bronze_declared.py`.
"""

import json
import sys
from pathlib import Path

import pytest
from medalflow.api import compile
from medalflow.medallion.types import TableInfo
from medalflow.settings import main as settings_main
from medalflow.settings.main import MedalflowSettings

from tests.conftest import OFFLINE_ENV

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
ENV_EXAMPLE = Path(__file__).resolve().parents[2] / ".env.example"

BRONZE_ENV = OFFLINE_ENV | {"MEDALFLOW_MODELS_PACKAGE": "sample_project"}


def _settings(**overrides) -> MedalflowSettings:
    return MedalflowSettings(
        source_system="sap",
        ds_env="dev",
        name="fin",
        compute={"lake_database_name": "lakedb"},
        **overrides,
    )


# --- the setting -----------------------------------------------------------


def test_introspection_is_off_by_default():
    """Declared is the default mode; introspection is opted into."""
    assert _settings().bronze_introspection is False


def test_introspection_can_be_turned_on():
    assert _settings(bronze_introspection=True).bronze_introspection is True


def test_env_example_documents_the_introspection_switch():
    documented = set()
    for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        line = line.lstrip("#").strip()
        if line.startswith("MEDALFLOW_") and "=" in line:
            documented.add(line.split("=", 1)[0].strip())

    assert "MEDALFLOW_BRONZE_INTROSPECTION" in documented


# --- the bronze plan -------------------------------------------------------


@pytest.fixture
def sample_project_settings(monkeypatch):
    """Point MedalFlow at the sample project the way a real deployment would."""
    monkeypatch.syspath_prepend(str(FIXTURES))
    for name in [m for m in sys.modules if m.split(".")[0] == "sample_project"]:
        del sys.modules[name]

    for key, value in BRONZE_ENV.items():
        monkeypatch.setenv(key, value)

    settings = settings_main.get_settings(force_reload=True)
    try:
        yield settings
    finally:
        settings_main._settings = None


@pytest.fixture
def no_warehouse(monkeypatch):
    """Explodes if anything reaches for a warehouse (D6)."""

    def _explode(settings, schema):
        raise AssertionError("LakeDatabase was constructed")

    monkeypatch.setattr("medalflow.medallion.bronze.sequencer.LakeDatabase", _explode)


def _object_names(plan):
    return sorted(operation.object_name for stage in plan.stages for operation in stage.operations)


def test_the_declared_models_become_the_plan(sample_project_settings, no_warehouse):
    result = compile("layer:bronze")

    assert _object_names(result.plan) == ["Customers", "Orders"]


def test_the_plan_compiles_without_a_warehouse(sample_project_settings, no_warehouse):
    """The whole point of Decision 6 part 2: `no_warehouse` would have fired."""
    assert compile("layer:bronze").plan.total_queries == 2


def test_the_bronze_plan_is_serialisable(sample_project_settings, no_warehouse):
    """A plan an author or an agent can read is a plan that survives JSON."""
    payload = json.loads(json.dumps(compile("layer:bronze").plan.to_dict()))

    assert payload["total_queries"] == 2
    assert sorted(
        operation["object_name"] for stage in payload["stages"] for operation in stage["operations"]
    ) == ["Customers", "Orders"]


def test_one_bronze_table_can_be_selected_by_name(sample_project_settings, no_warehouse):
    """Bronze models are named per table, so the selector is the selection.

    The old entry point took a `table_names` list, which used to be
    `",".join(...)` in and `.split(",")` out -- a round trip through a string
    that could only lose table names containing a comma.
    """
    result = compile("Orders")

    assert _object_names(result.plan) == ["Orders"]


def test_an_unconfigured_bronze_package_does_not_fall_back_to_a_warehouse(
    monkeypatch, no_warehouse
):
    """A project with no bronze package configured must say so, not introspect."""
    for key, value in OFFLINE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("MEDALFLOW_MODELS_PACKAGE", raising=False)
    settings_main.get_settings(force_reload=True)

    try:
        result = compile("layer:bronze")

        assert result.ok is False
        assert any(
            "MEDALFLOW_BRONZE_PACKAGE" in (error.suggestion or "") for error in result.errors
        )
        assert result.plan.total_queries == 0
    finally:
        settings_main._settings = None


# --- introspection, the opt-in alternative ---------------------------------


class _LakeDatabase:
    """Stands in for the warehouse; records how it was asked."""

    def __init__(self, settings, schema="dbo"):
        self.schema = schema
        _LAKE_CALLS.append(schema)

    def get_tables(self, table_names=None):
        return [
            TableInfo(table_name="Customers", schema_name="dbo", full_table_name="dbo.Customers"),
            TableInfo(table_name="Invoices", schema_name="dbo", full_table_name="dbo.Invoices"),
        ]


_LAKE_CALLS: list[str] = []


@pytest.fixture
def introspecting(monkeypatch):
    """Turn the mode on, with the table list faked rather than queried.

    No bronze package is configured, deliberately: the mode comes from the
    setting, not from what a package walk could find.
    """
    _LAKE_CALLS.clear()
    monkeypatch.setattr("medalflow.medallion.bronze.metadata_discovery.LakeDatabase", _LakeDatabase)

    for key, value in OFFLINE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("MEDALFLOW_MODELS_PACKAGE", raising=False)
    monkeypatch.setenv("MEDALFLOW_BRONZE_INTROSPECTION", "true")

    settings = settings_main.get_settings(force_reload=True)
    try:
        yield settings
    finally:
        settings_main._settings = None


def test_introspection_takes_over_when_it_is_configured(introspecting):
    """No bronze package is configured at all here, and that is fine."""
    result = compile("layer:bronze")

    assert _object_names(result.plan) == ["Customers", "Invoices"]


def test_each_introspected_table_is_its_own_model(introspecting):
    """The point of doing this in discovery rather than in one sequencer:
    a selector has models to match, so both modes behave identically."""
    models = compile("layer:bronze").models

    assert [model.name for model in models] == ["Customers", "Invoices"]
    assert {model.layer for model in models} == {"bronze"}
    assert {model.schema for model in models} == {"bronze"}


def test_a_selector_narrows_introspected_tables_the_same_way(introspecting):
    result = compile("Invoices")

    assert [model.name for model in result.models] == ["Invoices"]
    assert _object_names(result.plan) == ["Invoices"]


def test_only_the_bronze_layer_reaches_the_warehouse(introspecting):
    """Turning the flag on costs offline compile for the bronze layer only:
    silver and gold still answer from their packages.

    Note what this does *not* say. `compile()` discovers every layer and
    applies the selector afterwards, so even `compile("layer:silver")`
    introspects bronze. The mode is a property of the layer, not of the
    selector.
    """
    compile("*")

    assert _LAKE_CALLS == ["dbo"]

    _LAKE_CALLS.clear()
    compile("layer:silver")

    assert _LAKE_CALLS == ["dbo"]


def test_an_unreachable_warehouse_is_a_compile_error_not_a_crash(monkeypatch):
    """A mistyped connection string gets the same structured error surface as
    a mistyped package name."""

    class _Unreachable:
        def __init__(self, settings, schema="dbo"):
            raise OSError("could not connect to the warehouse")

    monkeypatch.setattr("medalflow.medallion.bronze.metadata_discovery.LakeDatabase", _Unreachable)
    for key, value in OFFLINE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("MEDALFLOW_BRONZE_INTROSPECTION", "true")
    settings_main.get_settings(force_reload=True)

    try:
        result = compile("layer:bronze")

        (error,) = (e for e in result.errors if e.error_type == "OSError")
        assert error.model is None
        assert "could not connect" in error.message
        assert "MEDALFLOW_BRONZE_INTROSPECTION" in error.suggestion
        assert result.ok is False
        assert result.plan.total_queries == 0
    finally:
        settings_main._settings = None


def test_the_default_mode_never_reaches_the_warehouse(sample_project_settings):
    """D6, and the example project depends on it. `_LAKE_CALLS` would have
    grown if discovery had introspected."""
    _LAKE_CALLS.clear()

    class _Explode(_LakeDatabase):
        def __init__(self, settings, schema="dbo"):
            raise AssertionError("LakeDatabase was constructed")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("medalflow.medallion.bronze.metadata_discovery.LakeDatabase", _Explode)
        patch.setattr("medalflow.medallion.bronze.sequencer.LakeDatabase", _Explode)

        assert _object_names(compile("layer:bronze").plan) == ["Customers", "Orders"]
