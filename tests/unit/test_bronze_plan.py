"""How the bronze entry point picks its mode (ADR 002, Decision 6).

A declared bronze model is the default and introspection is the documented
alternative, so `get_bronze_execution_plan` has to choose between them. It
chooses on configuration -- `bronze_introspection` -- and never on whether
models happen to be found: inferring the mode would mean a typo in
`MEDALFLOW_BRONZE_PACKAGE` silently falls back to hitting a warehouse, which is
the offline-compile guarantee (D6) failing quietly instead of loudly.
"""

import sys
from pathlib import Path

import pytest
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


# --- the entry point -------------------------------------------------------


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
    from medalflow.api.medallion import get_bronze_execution_plan

    plan = get_bronze_execution_plan(None)

    assert _object_names(plan) == ["Customers", "Orders"]


def test_the_plan_compiles_without_a_warehouse(sample_project_settings, no_warehouse):
    """The whole point of Decision 6 part 2: `no_warehouse` would have fired."""
    from medalflow.api.medallion import get_bronze_execution_plan

    assert get_bronze_execution_plan(None).total_queries == 2


def test_the_plan_this_entry_point_returns_is_serialisable(sample_project_settings, no_warehouse):
    """`create_plan_from_sequencers` gives the plan a plain dict as metadata,
    and `ExecutionPlan.to_dict` called `.to_dict()` on it -- so every plan the
    per-layer entry points return raised AttributeError when serialised, next
    to a `CompileResult` that serialises fine."""
    import json

    from medalflow.api.medallion import get_bronze_execution_plan

    payload = json.loads(json.dumps(get_bronze_execution_plan(None).to_dict()))

    assert payload["metadata"]["sequencers"] == ["Customers", "Orders"]
    assert payload["total_queries"] == 2


def test_the_selection_arrives_as_a_list(sample_project_settings, no_warehouse):
    """It used to be `",".join(...)` in and `.split(",")` out -- a round trip
    through a string that could only lose table names containing a comma."""
    from medalflow.api.medallion import get_bronze_execution_plan

    plan = get_bronze_execution_plan(["Orders"])

    assert _object_names(plan) == ["Orders"]


def test_an_unconfigured_bronze_package_does_not_fall_back_to_a_warehouse(monkeypatch):
    """A project with no bronze package configured must say so, not introspect."""
    from medalflow.api.medallion import get_bronze_execution_plan

    for key, value in OFFLINE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("MEDALFLOW_MODELS_PACKAGE", raising=False)
    settings_main.get_settings(force_reload=True)

    try:
        with pytest.raises(ValueError, match="MEDALFLOW_BRONZE_PACKAGE"):
            get_bronze_execution_plan(None)
    finally:
        settings_main._settings = None


# --- introspection, when it is asked for -----------------------------------


class _LakeDatabase:
    def __init__(self, settings, schema):
        self.schema = schema

    def get_tables(self, table_names=None):
        tables = [
            TableInfo(table_name="Customers", schema_name="dbo", full_table_name="dbo.Customers"),
            TableInfo(table_name="Invoices", schema_name="dbo", full_table_name="dbo.Invoices"),
        ]
        if not table_names:
            return tables
        return [table for table in tables if table.table_name in table_names]


@pytest.fixture
def introspecting_settings(monkeypatch):
    monkeypatch.setattr("medalflow.medallion.bronze.sequencer.LakeDatabase", _LakeDatabase)

    for key, value in OFFLINE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("MEDALFLOW_BRONZE_INTROSPECTION", "true")

    settings = settings_main.get_settings(force_reload=True)
    try:
        yield settings
    finally:
        settings_main._settings = None


def test_introspection_takes_over_when_it_is_configured(introspecting_settings):
    """No bronze package is configured at all here, and that is fine: the mode
    came from the setting, not from what discovery could find."""
    from medalflow.api.medallion import get_bronze_execution_plan

    plan = get_bronze_execution_plan(None)

    assert _object_names(plan) == ["Customers", "Invoices"]


def test_introspection_still_honours_the_selection(introspecting_settings):
    from medalflow.api.medallion import get_bronze_execution_plan

    plan = get_bronze_execution_plan(["Invoices"])

    assert _object_names(plan) == ["Invoices"]
