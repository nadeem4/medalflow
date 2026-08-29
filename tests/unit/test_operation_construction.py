"""Regression tests for operation construction (Phase 1, task 3).

Four defects, all of which made operation building fail at runtime:

1. Sequencers passed `schema=` to operation constructors, but every operation
   model declares the field as `schema_name`. Pydantic's default `extra="ignore"`
   dropped `schema` silently and then raised "schema_name Field required".
2. `OperationBuilder.create_operation_from_dict` called
   `attach_context(ctx, stage=..., position=...)`, but `attach_context` accepts
   only `ctx` — a TypeError on every deserialization that carried a context.
3. Both unknown-query-type fallbacks built `ExecuteSQL(sql="")`, which can never
   validate because `ExecuteSQL.sql` declares `min_length=1`. The "fallback"
   could only ever raise a confusing ValidationError.
"""

import logging

import pytest
from medalflow.constants.sql import QueryType
from medalflow.medallion.base.sequencer import _BaseSequencer
from medalflow.medallion.bronze.sequencer import BronzeSequencer
from medalflow.medallion.types import TableInfo
from medalflow.observability.context import ExecutionRequestContext
from medalflow.operations import Select
from medalflow.operations.builder import OperationBuilder
from medalflow.settings.main import MedalflowSettings
from medalflow.types.metadata import DiscoveredMethod, QueryMetadata


@pytest.fixture
def offline_env(monkeypatch):
    """`_BaseSequencer._init_feature_managers` resolves the settings singleton."""
    from medalflow.settings import main as settings_main

    from tests.conftest import OFFLINE_ENV

    for key, value in OFFLINE_ENV.items():
        monkeypatch.setenv(key, value)
    settings_main._settings = None
    try:
        yield
    finally:
        settings_main._settings = None


# --- 1. schema= vs schema_name= -------------------------------------------


def test_bronze_select_operation_sets_schema_name(offline_env):
    """bronze/sequencer.py:130 passed `schema=`, so schema_name was never set."""
    # A real constructor call: `lake_db` is lazy (D5), so this stays offline (D6).
    # The soft-delete convention is read off settings now (Phase 3, task 8);
    # unconfigured, it contributes no WHERE clause.
    sequencer = BronzeSequencer(
        MedalflowSettings(
            source_system="sap", ds_env="dev", name="fin", compute={"lake_database_name": "lakedb"}
        )
    )

    operation = sequencer._create_select_operation(
        TableInfo(table_name="Customer", schema_name="dbo", full_table_name="dbo.Customer")
    )

    assert isinstance(operation, Select)
    assert operation.schema_name == "dbo"


class _StubSequencer(_BaseSequencer):
    """Minimal sequencer that exercises `_get_queries` without any warehouse.

    `_BaseSequencer.__init__` needs live settings; this subclass supplies only
    the members `_get_queries` actually reads, keeping the test offline per
    Decision D6. `selection` is None: this test is about operation
    construction, so it selects everything.
    """

    def __init__(self):
        self.logger = logging.getLogger("stub-sequencer")
        self.selection = None

    def get_obj_name(self) -> str:
        return "StubModel"

    def get_layer_name(self) -> str:
        return "silver"

    def _get_method_source(self, method_name: str) -> str:
        return f"def {method_name}(self): ..."


def _discovered(sql="SELECT 1 AS n", **meta):
    metadata = QueryMetadata(
        type=QueryType.CREATE_TABLE,
        table_name="Customer",
        schema_name="silver",
        **meta,
    )
    return DiscoveredMethod("build_customer", lambda: sql, metadata, sql)


def test_base_sequencer_builds_operation_with_schema_name():
    """base/sequencer.py:476 passed `schema=`, so create_operation raised."""
    operations = _StubSequencer()._get_queries([_discovered()])

    assert len(operations) == 1
    assert operations[0].schema_name == "silver"
    assert operations[0].object_name == "Customer"


# --- 2. attach_context round trip -----------------------------------------


def test_operation_round_trip_preserves_context():
    """A serialized operation carrying `_cte_request_context` must rebuild.

    `ExecutionPlan.get_all_operations(serialize=True)` stamps `_cte_stage`,
    `_cte_position` and `_cte_request_context` onto each dict; rebuilding one
    raised `TypeError: attach_context() got an unexpected keyword argument
    'stage'`.
    """
    operation = Select(
        operation_type=QueryType.SELECT,
        schema_name="silver",
        object_name="Customer",
        columns=["*"],
    )
    ctx = ExecutionRequestContext(request_id="req-1")

    payload = operation.to_dict()
    payload["_cte_stage"] = 2
    payload["_cte_position"] = 0
    payload["_cte_request_context"] = ctx.model_dump()

    rebuilt = OperationBuilder.create_operation_from_dict(payload)

    assert rebuilt.schema_name == "silver"
    assert rebuilt.object_name == "Customer"
    assert rebuilt.context is not None
    assert rebuilt.logging_context.get("stage") == 2
    assert rebuilt.logging_context.get("position") == 0


def test_operation_round_trip_without_context_still_records_stage():
    operation = Select(
        operation_type=QueryType.SELECT,
        schema_name="silver",
        object_name="Customer",
        columns=["*"],
    )

    payload = operation.to_dict()
    payload["_cte_stage"] = 5

    rebuilt = OperationBuilder.create_operation_from_dict(payload)

    assert rebuilt.context is None
    assert rebuilt.logging_context.get("stage") == 5


# --- 3. unknown query type must raise, not build an invalid ExecuteSQL ------


def test_create_operation_rejects_unregistered_query_type(monkeypatch):
    registry = dict(OperationBuilder._registry)
    del registry[QueryType.SELECT]
    monkeypatch.setattr(OperationBuilder, "_registry", registry)

    with pytest.raises(ValueError, match="No operation class registered for query type 'SELECT'"):
        OperationBuilder.create_operation(
            query_type=QueryType.SELECT,
            schema_name="silver",
            object_name="Customer",
            columns=["*"],
        )


def test_create_operation_from_dict_rejects_unregistered_query_type(monkeypatch):
    registry = dict(OperationBuilder._registry)
    del registry[QueryType.SELECT]
    monkeypatch.setattr(OperationBuilder, "_registry", registry)

    with pytest.raises(ValueError, match="No operation class registered for query type 'SELECT'"):
        OperationBuilder.create_operation_from_dict(
            {
                "operation_type": QueryType.SELECT.value,
                "schema_name": "silver",
                "object_name": "Customer",
            }
        )


# --- the failure message must survive a plain-string query type ------------


def test_a_construction_failure_reports_the_underlying_validation_error():
    """The error handler raised on itself and swallowed the real message.

    `QueryMetadata.type` is stored as a plain `str` (`use_enum_values=True`), so
    every call from `_BaseSequencer._get_queries` passes one. The handler then
    did `query_type.value` and died with `'str' object has no attribute
    'value'` -- masking, for instance, the empty `schema_name` that actually
    failed.
    """
    with pytest.raises(ValueError, match="schema_name"):
        OperationBuilder.create_operation(
            query_type=QueryType.CREATE_TABLE.value,
            schema_name="",
            object_name="DimCustomer",
            select_query="SELECT 1 AS n",
        )


def test_an_unregistered_string_query_type_is_named_in_the_error(monkeypatch):
    """The same `.value` assumption, in the other message on this path."""
    registry = dict(OperationBuilder._registry)
    del registry[QueryType.SELECT]
    monkeypatch.setattr(OperationBuilder, "_registry", registry)

    with pytest.raises(ValueError, match="No operation class registered for query type 'SELECT'"):
        OperationBuilder.create_operation(
            query_type=QueryType.SELECT.value,
            schema_name="silver",
            object_name="Customer",
        )


# --- 4. full_object_name doubled the prefix separator ----------------------


def test_full_object_name_has_a_single_underscore_after_the_prefix(monkeypatch):
    """`settings.table_prefix` is already `f"{name}_"`, but `full_object_name`
    appended another underscore, so NAME=fin produced `silver.fin__Customers`
    in every log line and error message that used it."""
    import medalflow.settings

    settings = MedalflowSettings(
        source_system="sap",
        ds_env="dev",
        name="fin",
        compute={"lake_database_name": "lakedb"},
    )
    monkeypatch.setattr(medalflow.settings, "get_settings", lambda: settings)

    operation = Select(
        operation_type=QueryType.SELECT,
        schema_name="silver",
        object_name="Customers",
        columns=["*"],
    )

    assert settings.table_prefix == "fin_"
    assert operation.full_object_name == "silver.fin_Customers"


# --- engine_hint is stored as a string, not an enum -------------------------


def _select_with_engine_hint():
    from medalflow.constants.compute import EngineType

    return Select(
        operation_type=QueryType.SELECT,
        schema_name="gold",
        object_name="vw_Revenue",
        engine_hint=EngineType.SQL,
    )


def test_attaching_context_records_the_engine_hint():
    """`CTEBaseModel` sets `use_enum_values=True`, so the field holds `'sql'`.

    Both readers called `.value` on it, which a `str` does not have. Nothing
    exercised it: the sequencer sets `engine_hint` on every operation it
    builds, so the first plan attached to a context raised AttributeError.
    """
    operation = _select_with_engine_hint()
    ctx = ExecutionRequestContext.generate()

    operation.attach_context(ctx)

    assert ctx.attributes["engine_hint"] == "sql"


def test_telemetry_fields_report_the_engine_hint():
    assert _select_with_engine_hint().telemetry_fields()["operation.engine_hint"] == "sql"
