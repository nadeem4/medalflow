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

Nothing here hand-writes ``writes_to``: operations go through the real
``SynapseServerlessQueryBuilder`` and the real ``analyze_operations``. One test
forces ``extract_dependencies`` to raise, because the point of taking the
target from the operation is precisely that it survives a parse that fails --
and a real failure is a sqlglot version away rather than something a fixture
can conjure.
"""

import logging

import pytest
from medalflow.constants.sql import QueryType
from medalflow.medallion.orchestration.execution_orchestrator import ExecutionPlanOrchestrator
from medalflow.medallion.utils.sql_dependency_analyzer import SQLDependencyAnalyzer
from medalflow.operations import CreateOrAlterView, CreateTable, DropTable
from sqlglot.errors import ParseError


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


# --- the operation, not the generated SQL, owns the target -----------------


def test_the_declared_target_survives_sql_the_parser_cannot_read(analyzer, caplog, monkeypatch):
    """A parse failure costs the reads. It must never cost the target.

    Re-deriving `writes_to` by parsing SQL the query builder just generated is
    what made a sqlglot version bump able to erase an operation from the DAG.
    The operation declares its own target, so the target is taken from there.
    """
    operation = _silver_dim_customer()

    def _unreadable(sql):
        raise ParseError("sqlglot cannot read this statement")

    monkeypatch.setattr(analyzer, "extract_dependencies", _unreadable)

    with caplog.at_level(logging.ERROR):
        deps = analyzer.analyze_operations([operation])[operation]

    assert deps.writes_to == "silver.dimcustomer"
    assert deps.reads_from == set()
    assert [record for record in caplog.records if record.levelno >= logging.ERROR]


def test_the_declared_target_is_the_logical_name_not_the_prefixed_one(analyzer):
    """`silver.DimCustomer`, never `silver.fin_DimCustomer` (ADR 000)."""
    operation = _silver_dim_customer()

    assert analyzer.analyze_operations([operation])[operation].writes_to == "silver.dimcustomer"


def test_an_operation_that_produces_no_object_declares_no_target(analyzer):
    """A DROP removes an object; naming its target would invent an edge."""
    operation = DropTable(schema_name="silver", object_name="DimCustomer")

    assert analyzer.analyze_operations([operation])[operation].writes_to is None


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


# --- MedalFlow's own guarded DDL is not a user's parse failure -------------
#
# T-SQL has no `DROP EXTERNAL TABLE IF EXISTS` and no `CREATE SCHEMA IF NOT
# EXISTS`, so the builder emits a catalog-probe guard for both. sqlglot 23
# cannot parse either, and the analyzer used to report that as an unreadable
# statement -- four WARNINGs out of a compile with nothing wrong with it.
#
# The prelude is skipped quietly because it provably declares no edge: it
# probes `sys.` (already discarded, `_CATALOG_SCHEMA`) and its payload is a
# DROP or a CREATE SCHEMA (neither is a writer, `_WRITE_EXPRESSIONS`). A
# *user's* unreadable statement is a different thing entirely and stays loud.


def test_the_generated_recreate_guard_warns_about_nothing(analyzer, query_builder, caplog):
    """A clean compile must not accuse a healthy project of unreadable SQL."""
    sql = query_builder.build_query(_bronze_customers())

    assert sql.startswith("IF EXISTS (")  # the guard really is there

    with caplog.at_level(logging.DEBUG):
        deps = analyzer.extract_dependencies(sql)

    assert deps.reads_from == {"dbo.customers"}
    assert deps.writes_to == "bronze.customers"
    assert [record.msg for record in caplog.records if record.levelno >= logging.WARNING] == []


def test_a_guarded_drop_is_a_real_answer_rather_than_an_unreadable_one(
    analyzer, query_builder, caplog
):
    """The guard is the whole statement here, so it used to leave nothing
    readable at all -- which raised, and surfaced as an ERROR."""
    sql = query_builder.build_query(DropTable(schema_name="silver", object_name="DimCustomer"))

    with caplog.at_level(logging.DEBUG):
        deps = analyzer.extract_dependencies(sql)

    assert deps.reads_from == set()
    assert deps.writes_to is None
    assert [record.msg for record in caplog.records if record.levelno >= logging.WARNING] == []


def test_a_user_statement_the_parser_cannot_read_still_warns(analyzer, caplog):
    """The load-bearing half: `reads_from` is only ever derived by parsing, so
    a user's SELECT that will not parse means edges missing from the DAG."""
    sql = "SELECT * FROM silver.DimCustomer;\nSELECT FROM WHERE FROM"

    with caplog.at_level(logging.DEBUG):
        deps = analyzer.extract_dependencies(sql)

    assert deps.reads_from == {"silver.dimcustomer"}
    assert [record.msg for record in caplog.records if record.levelno >= logging.WARNING] == [
        "dependency.analyzer.statement_unreadable"
    ]
