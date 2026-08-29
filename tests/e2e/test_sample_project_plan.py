"""End-to-end plan generation over a sample project (Phase 1, task 10).

Everything a consuming team writes — decorated classes with methods returning
SQL — through discovery, sequencers, dependency extraction, DAG building and
into a validated ExecutionPlan. Entirely offline (Decision D6): no warehouse,
no network.

The fixture under tests/fixtures/sample_project is shaped like a real project:

    bronze.Customers ──> silver.DimCustomer ─┐
                                             ├─> silver.FactOrders ──> gold.vw_Revenue
    bronze.Orders ───────────────────────────┘

so the plan must contain the bronze->silver, silver->silver and silver->gold
edges. Every layer, bronze included, arrives through discovery.
"""

import sys
from pathlib import Path

import pytest
from medalflow.constants.sql import QueryType
from medalflow.medallion.bronze.metadata_discovery import BronzeMetadataDiscovery
from medalflow.medallion.gold.metadata_discovery import GoldMetadataDiscovery
from medalflow.medallion.orchestration.execution_orchestrator import ExecutionPlanOrchestrator
from medalflow.medallion.silver.metadata_discovery import SilverMetadataDiscovery
from medalflow.medallion.utils.execution_plan_builder import ExecutionPlanBuilder
from medalflow.medallion.utils.sql_dependency_analyzer import SQLDependencyAnalyzer
from medalflow.operations import CreateTable

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


@pytest.fixture(autouse=True)
def sample_project_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(FIXTURES))
    for name in [m for m in sys.modules if m.split(".")[0] == "sample_project"]:
        del sys.modules[name]


class _StubSettings:
    """Only what discovery reads. A real project supplies these from config."""

    def is_model_configured(self, model_name: str) -> bool:
        return model_name == "sales"


@pytest.fixture
def discovery():
    return SilverMetadataDiscovery("sample_project.silver", settings=_StubSettings())


@pytest.fixture
def gold_discovery():
    return GoldMetadataDiscovery("sample_project.gold", settings=_StubSettings())


@pytest.fixture
def bronze_discovery():
    return BronzeMetadataDiscovery("sample_project.bronze", settings=_StubSettings())


@pytest.fixture
def analyzer(offline_settings):
    return SQLDependencyAnalyzer(offline_settings)


@pytest.fixture
def orchestrator(offline_settings):
    return ExecutionPlanOrchestrator(offline_settings)


# --- discovery -------------------------------------------------------------


def test_discovery_finds_every_silver_model(discovery):
    transformations = discovery.discover_all_transformations(force_refresh=True)

    assert sorted(t.name for t in transformations) == [
        "DimCustomer",
        "FactOrders",
    ]
    assert {t.model for t in transformations} == {"sales"}


def test_discovery_finds_the_bronze_models(bronze_discovery):
    """Bronze is walked too. Nothing here reaches a warehouse: the models
    declare their own tables (D6)."""
    discovered = bronze_discovery.discover_all(force_refresh=True)

    assert sorted(model.name for model in discovered) == ["Customers", "Orders"]


def test_discovery_finds_the_gold_model(gold_discovery):
    """Gold is walked, not hand-imported. `is_model_configured` is not applied
    here: `_StubSettings` answers True only for 'sales', and gold declares no
    model at all."""
    discovered = gold_discovery.discover_all(force_refresh=True)

    assert [model.name for model in discovered] == ["Revenue"]


def test_discovered_sequencer_classes_are_instantiable_types(discovery):
    transformations = discovery.discover_all_transformations(force_refresh=True)

    for transformation in transformations:
        assert isinstance(transformation.sequencer_class, type)
        assert hasattr(transformation.sequencer_class, "_silver_metadata")


# --- the models' SQL reaches operations ------------------------------------


def _operations_from_sample_project(settings):
    """Build the five operations the sample project describes.

    Every layer arrives the same way a real project reaches it: discovery walks
    the package, and each discovered model's `get_queries()` builds its
    operations. The hand-built `CreateTable`/`CreateOrAlterView` that used to
    stand in for silver and gold are gone — they read `metadata.schema_name`
    off the decorator, which is precisely the resolution step `_get_queries`
    now owns, so this suite would have described the models rather than running
    them.
    """
    bronze = BronzeMetadataDiscovery("sample_project.bronze", settings=_StubSettings())
    silver = SilverMetadataDiscovery("sample_project.silver", settings=_StubSettings())
    gold = GoldMetadataDiscovery("sample_project.gold", settings=_StubSettings())

    return [
        operation
        for discovery in (bronze, silver, gold)
        for model in sorted(discovery.discover_all(force_refresh=True), key=lambda m: m.name)
        for operation in model.sequencer_class(settings).get_queries()
    ]


def test_model_methods_produce_the_expected_sql():
    from sample_project.silver.orders import FactOrders

    sql = FactOrders.build_fact_orders(FactOrders)

    assert sql == (
        "SELECT o.OrderId, c.CustomerId "
        "FROM bronze.Orders o "
        "JOIN silver.DimCustomer c ON c.CustomerId = o.CustomerId"
    )


# --- dependency extraction over the real model SQL -------------------------


def test_dependencies_are_extracted_from_the_model_sql(analyzer, gold_discovery):
    from sample_project.silver.orders import FactOrders

    Revenue = gold_discovery.discover_all(force_refresh=True)[0].sequencer_class

    orders = analyzer.extract_dependencies(FactOrders.build_fact_orders(FactOrders))
    revenue = analyzer.extract_dependencies(Revenue.build_revenue_view(Revenue))

    assert "silver.dimcustomer" in orders.reads_from
    assert "bronze.orders" in orders.reads_from
    assert "silver.factorders" in revenue.reads_from


# --- the whole plan --------------------------------------------------------


@pytest.fixture
def plan(orchestrator, offline_settings):
    """The real path: operations -> query builder -> analyzer -> DAG -> plan.

    Nothing here supplies dependencies. `create_execution_plan` renders each
    operation with the configured query builder and reads the edges back out
    of that SQL, guard, table prefix and all.
    """
    return orchestrator.create_execution_plan(
        operations=_operations_from_sample_project(offline_settings),
        sequencer_name="sample_project",
    )


def test_plan_has_one_stage_per_dependency_level(plan):
    assert plan.total_queries == 5
    assert len(plan.stages) == 4

    staged = [
        sorted(operation.object_name for operation in stage.operations) for stage in plan.stages
    ]
    # Both bronze tables are independent, so they share the first stage.
    assert staged == [["Customers", "Orders"], ["DimCustomer"], ["FactOrders"], ["vw_Revenue"]]


def test_every_operation_lands_in_its_models_declared_schema(plan):
    """No `@query_metadata` method in the fixture names a schema.

    Each model declares one, and its methods inherit it — which is the whole
    point of `schema` being on the layer decorators. If the inheritance broke,
    these operations could not be built at all, so this pins *where* they land.
    """
    schemas = {
        operation.object_name: operation.schema_name
        for stage in plan.stages
        for operation in stage.operations
    }

    assert schemas == {
        "Customers": "bronze",
        "Orders": "bronze",
        "DimCustomer": "silver",
        "FactOrders": "silver",
        "vw_Revenue": "gold",
    }


def test_plan_contains_the_silver_to_silver_edge(plan):
    by_name = {
        operation.object_name: operation._dag_id
        for stage in plan.stages
        for operation in stage.operations
    }

    assert by_name["DimCustomer"] in plan.dependency_graph[by_name["FactOrders"]]


def test_plan_contains_the_silver_to_gold_edge(plan):
    by_name = {
        operation.object_name: operation._dag_id
        for stage in plan.stages
        for operation in stage.operations
    }

    assert by_name["FactOrders"] in plan.dependency_graph[by_name["vw_Revenue"]]


def test_plan_contains_the_bronze_to_silver_edge(plan):
    by_name = {
        operation.object_name: operation._dag_id
        for stage in plan.stages
        for operation in stage.operations
    }

    assert by_name["Customers"] in plan.dependency_graph[by_name["DimCustomer"]]
    assert by_name["Orders"] in plan.dependency_graph[by_name["FactOrders"]]


def test_the_bronze_model_reads_its_declared_source_table(plan):
    """`Orders` lands `dbo.SalesOrders` as `bronze.Orders`, so the edge into
    silver has to match on the target name, not the source's."""
    orders = next(
        operation
        for stage in plan.stages
        for operation in stage.operations
        if operation.object_name == "Orders"
    )

    assert orders.schema_name == "bronze"
    assert orders.select_query == "SELECT * FROM [dbo].[SalesOrders]"


def test_plan_validates(plan):
    assert ExecutionPlanBuilder().validate_plan(plan) is True


# --- generated SQL ---------------------------------------------------------


@pytest.fixture
def query_builder(offline_settings):
    from medalflow.query_builder.factory import create_query_builder

    return create_query_builder()


def test_the_configured_builder_is_synapse_serverless(query_builder):
    assert type(query_builder).__name__ == "SynapseServerlessQueryBuilder"


def test_generated_sql_for_the_silver_model(query_builder):
    from sample_project.silver.customers import DimCustomer

    operation = CreateTable(
        operation_type=QueryType.CREATE_TABLE,
        schema_name="silver",
        object_name="DimCustomer",
        select_query=DimCustomer.build_dim_customer(DimCustomer),
        recreate=True,
    )

    assert query_builder.build_query(operation) == (
        "IF EXISTS (SELECT * FROM sys.external_tables "
        "WHERE object_id = OBJECT_ID('[silver].[fin_DimCustomer]'))\n"
        "    DROP EXTERNAL TABLE [silver].[fin_DimCustomer];\n"
        "CREATE EXTERNAL TABLE [silver].[fin_DimCustomer]\n"
        "WITH (\n"
        "    DATA_SOURCE = ds_fin_proc,\n"
        "    LOCATION = 'silver/DimCustomer',\n"
        "    FILE_FORMAT = parquet_file_format\n"
        ")\n"
        "AS SELECT CustomerId, Name FROM bronze.Customers"
    )


def test_generated_sql_quotes_and_prefixes_the_bronze_source(query_builder):
    from medalflow.operations import Select

    operation = Select(
        operation_type=QueryType.SELECT,
        schema_name="bronze",
        object_name="Customers",
    )

    assert query_builder.build_query(operation) == "SELECT * FROM [bronze].[fin_Customers]"
