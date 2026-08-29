"""End-to-end plan generation over a sample project (Phase 1, task 10).

Everything a consuming team writes — decorated classes with methods returning
SQL — through discovery, sequencers, dependency extraction, DAG building and
into a validated ExecutionPlan. Entirely offline (Decision D6): no warehouse,
no network.

The fixture under tests/fixtures/sample_project is shaped like a real project:

    bronze.Customers ─┐
                      ├─> silver.DimCustomer ──> silver.FactOrders ──> gold.vw_Revenue
    bronze.Orders ────┘

so the plan must contain both the silver->silver and the silver->gold edge.
"""

import sys
from pathlib import Path

import pytest
from medalflow.constants.sql import QueryType
from medalflow.medallion.orchestration.execution_orchestrator import ExecutionPlanOrchestrator
from medalflow.medallion.silver.metadata_discovery import SilverMetadataDiscovery
from medalflow.medallion.utils.execution_plan_builder import ExecutionPlanBuilder
from medalflow.medallion.utils.sql_dependency_analyzer import SQLDependencyAnalyzer
from medalflow.operations import CreateOrAlterView, CreateTable

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
def analyzer(offline_settings):
    return SQLDependencyAnalyzer(offline_settings)


@pytest.fixture
def orchestrator(offline_settings):
    return ExecutionPlanOrchestrator(offline_settings)


# --- discovery -------------------------------------------------------------


def test_discovery_finds_every_silver_model(discovery):
    transformations = discovery.discover_all_transformations(force_refresh=True)

    assert sorted(t.name for t in transformations) == [
        "usp_load_dim_customer",
        "usp_load_fact_orders",
    ]
    assert {t.model for t in transformations} == {"sales"}


def test_discovered_sequencer_classes_are_instantiable_types(discovery):
    transformations = discovery.discover_all_transformations(force_refresh=True)

    for transformation in transformations:
        assert isinstance(transformation.sequencer_class, type)
        assert hasattr(transformation.sequencer_class, "_silver_metadata")


# --- the models' SQL reaches operations ------------------------------------


def _operations_from_sample_project():
    """Build the four operations the sample project describes.

    Sequencer construction resolves live settings, so the operations are built
    from each model's decorated methods directly — the same SQL and metadata
    discovery hands to `_get_queries`.
    """
    from sample_project.gold.revenue import Revenue
    from sample_project.silver.customers import DimCustomer
    from sample_project.silver.orders import FactOrders

    bronze = CreateTable(
        operation_type=QueryType.CREATE_TABLE,
        schema_name="bronze",
        object_name="Customers",
        select_query="SELECT * FROM dbo.Customers",
        recreate=True,
    )

    operations = [bronze]
    for model, method_name in (
        (DimCustomer, "build_dim_customer"),
        (FactOrders, "build_fact_orders"),
        (Revenue, "build_revenue_view"),
    ):
        method = getattr(model, method_name)
        metadata = method._query_metadata
        if metadata.type == QueryType.CREATE_OR_ALTER_VIEW:
            operations.append(
                CreateOrAlterView(
                    schema_name=metadata.schema_name,
                    object_name=metadata.table_name,
                    select_query=method(model),
                )
            )
        else:
            operations.append(
                CreateTable(
                    operation_type=QueryType.CREATE_TABLE,
                    schema_name=metadata.schema_name,
                    object_name=metadata.table_name,
                    select_query=method(model),
                    recreate=True,
                )
            )
    return operations


def test_model_methods_produce_the_expected_sql():
    from sample_project.silver.orders import FactOrders

    sql = FactOrders.build_fact_orders(FactOrders)

    assert sql == (
        "SELECT o.OrderId, c.CustomerId "
        "FROM bronze.Orders o "
        "JOIN silver.DimCustomer c ON c.CustomerId = o.CustomerId"
    )


# --- dependency extraction over the real model SQL -------------------------


def test_dependencies_are_extracted_from_the_model_sql(analyzer):
    from sample_project.gold.revenue import Revenue
    from sample_project.silver.orders import FactOrders

    orders = analyzer.extract_dependencies(FactOrders.build_fact_orders(FactOrders))
    revenue = analyzer.extract_dependencies(Revenue.build_revenue_view(Revenue))

    assert "silver.dimcustomer" in orders.reads_from
    assert "bronze.orders" in orders.reads_from
    assert "silver.factorders" in revenue.reads_from


# --- the whole plan --------------------------------------------------------


@pytest.fixture
def plan(orchestrator):
    """The real path: operations -> query builder -> analyzer -> DAG -> plan.

    Nothing here supplies dependencies. `create_execution_plan` renders each
    operation with the configured query builder and reads the edges back out
    of that SQL, guard, table prefix and all.
    """
    return orchestrator.create_execution_plan(
        operations=_operations_from_sample_project(), sequencer_name="sample_project"
    )


def test_plan_has_one_stage_per_dependency_level(plan):
    assert plan.total_queries == 4
    assert len(plan.stages) == 4

    staged = [[operation.object_name for operation in stage.operations] for stage in plan.stages]
    assert staged == [["Customers"], ["DimCustomer"], ["FactOrders"], ["vw_Revenue"]]


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
