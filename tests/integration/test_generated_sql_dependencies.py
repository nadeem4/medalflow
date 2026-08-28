"""Dependency extraction over the SQL the query builder actually emits.

Phase 3.5. Every earlier test fed the analyzer either bare model SQL or a
hand-written ``writes_to``, so two defects on the real code path survived three
phases:

1. ``CreateTable.recreate`` defaults to True, so the Synapse builder wraps the
   CETAS in ``IF EXISTS (...) DROP EXTERNAL TABLE ...;``. sqlglot parses the
   whole thing as one ``IfBlock``, so root-only target extraction returned
   ``None``: the operation never became a writer and could never be anyone's
   dependency.
2. Generated names carry the deployment table prefix
   (``[silver].[fin_DimCustomer]``) while a model author writes
   ``silver.DimCustomer``, so the two sides could never match (ADR 000).

Nothing here hand-writes ``writes_to`` or stubs the analyzer: operations go
through the real ``SynapseServerlessQueryBuilder`` and the real
``analyze_operations``.
"""

import pytest
from medalflow.constants.sql import QueryType
from medalflow.medallion.orchestration.execution_orchestrator import ExecutionPlanOrchestrator
from medalflow.medallion.utils.sql_dependency_analyzer import SQLDependencyAnalyzer
from medalflow.operations import CreateOrAlterView, CreateTable


@pytest.fixture
def analyzer(offline_settings):
    return SQLDependencyAnalyzer(offline_settings)


@pytest.fixture
def query_builder(offline_settings):
    from medalflow.query_builder.factory import create_query_builder

    return create_query_builder()


def _bronze_customers():
    return CreateTable(
        operation_type=QueryType.CREATE_TABLE,
        schema_name="bronze",
        object_name="Customers",
        select_query="SELECT * FROM dbo.Customers",
        recreate=True,
    )


def _silver_dim_customer():
    return CreateTable(
        operation_type=QueryType.CREATE_TABLE,
        schema_name="silver",
        object_name="DimCustomer",
        select_query="SELECT CustomerId, Name FROM bronze.Customers",
        recreate=True,
    )


def _gold_revenue_view():
    return CreateOrAlterView(
        operation_type=QueryType.CREATE_OR_ALTER_VIEW,
        schema_name="gold",
        object_name="vw_Revenue",
        select_query="SELECT CustomerId FROM silver.DimCustomer",
    )


# --- defect 1: the recreate guard hid the target ---------------------------


def test_the_recreate_guard_does_not_hide_the_target(analyzer, query_builder):
    sql = query_builder.build_query(_bronze_customers())

    assert sql.startswith("IF EXISTS (")  # the guard really is there
    assert analyzer.extract_dependencies(sql).writes_to == "bronze.customers"


def test_the_guard_does_not_leak_into_reads_from(analyzer, query_builder):
    deps = analyzer.extract_dependencies(query_builder.build_query(_bronze_customers()))

    assert deps.reads_from == {"dbo.customers"}


def test_a_view_target_is_extracted(analyzer, query_builder):
    deps = analyzer.extract_dependencies(query_builder.build_query(_gold_revenue_view()))

    assert deps.writes_to == "gold.vw_revenue"
    assert deps.reads_from == {"silver.dimcustomer"}


# --- defect 2: the deployment prefix never matched -------------------------


def test_the_generated_prefix_is_normalised_away(analyzer, query_builder):
    """`[silver].[fin_DimCustomer]` and a model's `silver.DimCustomer` are one table."""
    generated = query_builder.build_query(_silver_dim_customer())

    assert "[silver].[fin_DimCustomer]" in generated  # the prefix really is there
    assert analyzer.extract_dependencies(generated).writes_to == "silver.dimcustomer"


def test_schemas_that_skip_the_prefix_are_left_alone(analyzer, query_builder):
    """`gold` is in skip_prefix_on_schema, so nothing may be stripped there."""
    generated = query_builder.build_query(_gold_revenue_view())

    assert "[gold].[vw_Revenue]" in generated
    assert analyzer.extract_dependencies(generated).writes_to == "gold.vw_revenue"


# --- the whole path: real builder, real analyzer, real DAG -----------------


def test_edges_form_across_layers_over_generated_sql(offline_settings):
    operations = [_bronze_customers(), _silver_dim_customer(), _gold_revenue_view()]

    plan = ExecutionPlanOrchestrator(offline_settings).create_execution_plan(
        operations=operations, sequencer_name="integration"
    )

    by_name = {
        operation.object_name: operation._dag_id
        for stage in plan.stages
        for operation in stage.operations
    }

    assert by_name["Customers"] in plan.dependency_graph[by_name["DimCustomer"]]
    assert by_name["DimCustomer"] in plan.dependency_graph[by_name["vw_Revenue"]]
    assert [[op.object_name for op in stage.operations] for stage in plan.stages] == [
        ["Customers"],
        ["DimCustomer"],
        ["vw_Revenue"],
    ]
