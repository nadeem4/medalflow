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

from core.constants.sql import QueryType
from core.medallion.base.sequencer import _BaseSequencer
from core.medallion.bronze.sequencer import BronzeSequencer
from core.medallion.types import TableInfo
from core.observability.context import ExecutionRequestContext
from core.operations import Select
from core.operations.builder import OperationBuilder
from core.types.metadata import DiscoveredMethod, QueryMetadata


# --- 1. schema= vs schema_name= -------------------------------------------


def test_bronze_select_operation_sets_schema_name():
    """bronze/sequencer.py:130 passed `schema=`, so schema_name was never set."""
    sequencer = BronzeSequencer.__new__(BronzeSequencer)
    sequencer.source_schema = "dbo"

    operation = sequencer._create_select_operation(
        TableInfo(table_name="Customer", schema_name="dbo", full_table_name="dbo.Customer")
    )

    assert isinstance(operation, Select)
    assert operation.schema_name == "dbo"


class _StubSequencer(_BaseSequencer):
    """Minimal sequencer that exercises `_get_queries` without any warehouse.

    `_BaseSequencer.__init__` needs live settings; this subclass supplies only
    the four members `_get_queries` actually reads, keeping the test offline
    per Decision D6.
    """

    def __init__(self):
        self.logger = logging.getLogger("stub-sequencer")

    def get_obj_name(self) -> str:
        return "StubModel"

    def get_layer_name(self) -> str:
        return "silver"

    def _get_method_source(self, method_name: str) -> str:
        return "def %s(self): ..." % method_name


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
