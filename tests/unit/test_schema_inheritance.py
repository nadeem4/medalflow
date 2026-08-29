"""A class's declared `schema` is the default for its own query methods.

ADR 002 D2 gives every layer a `schema` naming what the model writes. Stored
and never read, that is the "parameter that lied" shape this phase exists to
delete -- so the declaration is the default `schema_name` for the class's own
`@query_metadata` methods. A method that states one still wins: explicit beats
inherited.

The resolution happens where the operation is built, which is *after*
`_transform_query_result`. That ordering is load-bearing in silver and is
pinned below.
"""

import pytest
from medalflow.constants.sql import QueryType
from medalflow.medallion.base.decorators import query_metadata
from medalflow.medallion.gold import gold_metadata
from medalflow.medallion.gold.sequencer import GoldSequencer
from medalflow.medallion.silver import silver_metadata
from medalflow.medallion.silver.sequencer import SilverTransformationSequencer
from medalflow.settings.main import MedalflowSettings


def _settings(**conventions):
    return MedalflowSettings(
        source_system="sap",
        ds_env="dev",
        name="fin",
        compute={"lake_database_name": "lakedb"},
        conventions=conventions,
    )


@pytest.fixture(autouse=True)
def offline_env(monkeypatch):
    """`_init_feature_managers` still resolves global settings at construction."""
    from medalflow.settings import main as settings_main

    from tests.conftest import OFFLINE_ENV

    for key, value in OFFLINE_ENV.items():
        monkeypatch.setenv(key, value)
    settings_main._settings = None
    try:
        yield
    finally:
        settings_main._settings = None


def _by_object(operations):
    return {operation.object_name: operation for operation in operations}


# --- the default reaches the operation, in both layers that build one -------


@silver_metadata(name="DimCustomer", schema="silver", model="sales")
class _InheritingSilver(SilverTransformationSequencer):
    @query_metadata(type=QueryType.CREATE_TABLE, table_name="DimCustomer")
    def build_dim_customer(self) -> str:
        return "SELECT CustomerId FROM bronze.Customers"

    @query_metadata(type=QueryType.CREATE_TABLE, table_name="StageCustomer", schema_name="temp")
    def build_stage(self) -> str:
        return "SELECT CustomerId FROM bronze.Customers"


@gold_metadata(name="Revenue", schema="gold")
class _InheritingGold(GoldSequencer):
    @query_metadata(type=QueryType.CREATE_OR_ALTER_VIEW, table_name="vw_Revenue")
    def build_revenue(self) -> str:
        return "SELECT 1 AS Revenue FROM silver.FactOrders"

    @query_metadata(
        type=QueryType.CREATE_OR_ALTER_VIEW, table_name="vw_Draft", schema_name="sandbox"
    )
    def build_draft(self) -> str:
        return "SELECT 1 AS Draft FROM silver.FactOrders"


LAYERS = [
    (_InheritingSilver, "silver", "DimCustomer", "StageCustomer", "temp"),
    (_InheritingGold, "gold", "vw_Revenue", "vw_Draft", "sandbox"),
]
IDS = ["silver", "gold"]


@pytest.mark.parametrize(
    "sequencer_class,declared,inheriting,explicit,explicit_schema", LAYERS, ids=IDS
)
def test_a_method_without_a_schema_takes_the_classs_schema(
    sequencer_class, declared, inheriting, explicit, explicit_schema
):
    operations = _by_object(sequencer_class(_settings()).get_queries())

    assert operations[inheriting].schema_name == declared


@pytest.mark.parametrize(
    "sequencer_class,declared,inheriting,explicit,explicit_schema", LAYERS, ids=IDS
)
def test_a_method_that_states_a_schema_keeps_it(
    sequencer_class, declared, inheriting, explicit, explicit_schema
):
    """Explicit beats inherited, or the declaration is a silent override."""
    operations = _by_object(sequencer_class(_settings()).get_queries())

    assert operations[explicit].schema_name == explicit_schema


@pytest.mark.parametrize(
    "sequencer_class,declared,inheriting,explicit,explicit_schema", LAYERS, ids=IDS
)
def test_the_operations_metadata_agrees_with_the_operation(
    sequencer_class, declared, inheriting, explicit, explicit_schema
):
    """Two fields for one schema must not disagree."""
    operations = _by_object(sequencer_class(_settings()).get_queries())

    assert operations[inheriting].metadata.schema_name == declared


# --- an undeclared class is the status quo ---------------------------------


class _UndeclaredGold(GoldSequencer):
    """No `@gold_metadata`, so nothing to inherit."""

    @query_metadata(type=QueryType.CREATE_OR_ALTER_VIEW, table_name="vw_Orphan")
    def build_orphan(self) -> str:
        return "SELECT 1 AS One"


def test_a_method_with_no_schema_and_no_declaration_still_raises():
    """Unchanged: an operation needs a schema and there is nowhere to get one.

    The default must not invent one -- not the layer name, not `dbo`. There is
    no declaration, so the author hears about it.
    """
    with pytest.raises(ValueError, match="schema_name"):
        _UndeclaredGold(_settings()).get_queries()


# --- ordering against silver's detail-table rewrite ------------------------


DETAIL = {"table_suffix": "Detail", "source_schema": "temp", "target_schema": "silver"}


@silver_metadata(name="Staging", schema="temp", model="sales")
class _StagingSilver(SilverTransformationSequencer):
    """A class whose declared schema *is* the detail convention's source schema.

    The pathological case: if the class default were applied before
    `_transform_query_result`, every schema-less method here would start
    matching the detail convention and be rewritten into the target schema.
    """

    @query_metadata(type=QueryType.CREATE_TABLE, table_name="OrderDetail", schema_name="temp")
    def build_staged_order(self) -> str:
        return "SELECT OrderId AS OrderId FROM bronze.Orders"

    @query_metadata(type=QueryType.CREATE_TABLE, table_name="ItemDetail")
    def build_inherited_item(self) -> str:
        return "SELECT ItemId AS ItemId FROM bronze.Items"


def test_the_class_schema_is_applied_after_the_detail_rewrite():
    """The rewrite keys on what the *method* declared, not what it inherits.

    `build_staged_order` says `temp` and is promoted to `silver.Order`.
    `build_inherited_item` says nothing; it is not a staged detail table, so it
    is not promoted -- it simply lands in the class's own schema.
    """
    operations = _by_object(_StagingSilver(_settings(detail_tables=DETAIL)).get_queries())

    assert sorted(operations) == ["ItemDetail", "Order"]
    assert operations["Order"].schema_name == "silver"
    assert operations["ItemDetail"].schema_name == "temp"


def test_the_detail_rewrites_target_schema_is_not_overwritten_by_the_default():
    """The rewrite yields a non-empty schema, so the default must leave it be."""
    operations = _by_object(_StagingSilver(_settings(detail_tables=DETAIL)).get_queries())

    assert operations["Order"].metadata.schema_name == "silver"
