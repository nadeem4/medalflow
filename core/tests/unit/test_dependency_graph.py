"""Regression tests for dependency extraction and DAG building (Phase 1, tasks 4 and 5).

Two independent defects meant the DAG has never had a single edge:

1. `_extract_ctes_sqlglot` did `Set([...])` — `typing.Set` is not instantiable,
   so *every* call to `extract_dependencies` raised TypeError. The handler in
   `analyze_operations` then read `operation.schema` (no such field), raising
   AttributeError from inside the `except` block.
2. `SQLDependencies.reads_from` was `Dict[schema, Set[table]]` while
   `table_to_operation` is keyed by qualified `"schema.table"` names. The DAG
   builder iterated `reads_from` — yielding *schema names* — and tested them for
   membership in a dict of qualified names, so the test could never be true.
   Every operation became an isolated node.

Both are now settled on one shape: flat sets of fully-qualified lowercase
`"schema.table"` strings, matched globally so a silver model reading
`bronze.customers` forms a real cross-layer edge.
"""

import pytest
from medalflow.constants.sql import QueryType
from medalflow.medallion.orchestration.operation_dag_builder import OperationDAGBuilder
from medalflow.medallion.utils.sql_dependency_analyzer import SQLDependencyAnalyzer
from medalflow.operations import Select
from medalflow.types.metadata import SQLDependencies


@pytest.fixture
def analyzer():
    """Analyzer bound to the T-SQL dialect without booting live settings (D6)."""
    instance = SQLDependencyAnalyzer.__new__(SQLDependencyAnalyzer)
    instance.settings = None
    instance.dialect = "tsql"
    instance.table_prefix = ""
    return instance


# --- task 4: extraction actually runs -------------------------------------


def test_extracts_source_tables_from_select(analyzer):
    deps = analyzer.extract_dependencies(
        "SELECT c.id FROM bronze.Customers c JOIN bronze.Orders o ON o.cid = c.id"
    )

    assert deps.reads_from == {"bronze.customers", "bronze.orders"}
    assert deps.writes_to is None


def test_cte_aliases_are_not_reported_as_source_tables(analyzer):
    """`Set([...])` raised TypeError here, so CTE exclusion never ran."""
    deps = analyzer.extract_dependencies(
        """
        WITH recent AS (SELECT * FROM staging.temp)
        INSERT INTO silver.FactSales
        SELECT * FROM recent JOIN dim.Products p ON recent.pid = p.id
        """
    )

    assert "recent" not in deps.reads_from
    assert {"staging.temp", "dim.products"} <= deps.reads_from
    assert deps.writes_to == "silver.factsales"


def test_unqualified_table_is_still_reported(analyzer):
    deps = analyzer.extract_dependencies("SELECT * FROM Customers")

    assert deps.reads_from == {"customers"}


# --- task 5: one shape, matched globally ----------------------------------


def _select(schema, name):
    return Select(
        operation_type=QueryType.SELECT,
        schema_name=schema,
        object_name=name,
        columns=["*"],
    )


def test_cross_layer_edge_forms_between_bronze_and_silver():
    """A silver model reading bronze.customers must depend on the bronze writer."""
    bronze = _select("bronze", "Customers")
    silver = _select("silver", "DimCustomer")

    dependencies = {
        bronze: SQLDependencies(reads_from=set(), writes_to="bronze.customers"),
        silver: SQLDependencies(
            reads_from={"bronze.customers"}, writes_to="silver.dimcustomer"
        ),
    }

    builder = OperationDAGBuilder([bronze, silver], dependencies, settings=None)
    dag = builder.build_dag()

    assert dag.get_dependencies(silver._dag_id) == [bronze._dag_id]
    assert dag.get_dependencies(bronze._dag_id) == []


def test_both_writers_of_a_table_become_dependencies():
    """CREATE TABLE then INSERT INTO the same table: a reader needs both edges.

    `table_to_operation` was a plain dict keyed by table, so the second writer
    silently replaced the first and the CREATE -> reader edge disappeared.
    """
    create = _select("silver", "Fact")
    insert = _select("silver", "FactLoad")
    reader = _select("gold", "Report")

    dependencies = {
        create: SQLDependencies(reads_from=set(), writes_to="silver.fact"),
        insert: SQLDependencies(reads_from=set(), writes_to="silver.fact"),
        reader: SQLDependencies(reads_from={"silver.fact"}, writes_to="gold.report"),
    }

    builder = OperationDAGBuilder([create, insert, reader], dependencies, settings=None)
    dag = builder.build_dag()

    assert sorted(dag.get_dependencies(reader._dag_id)) == sorted(
        [create._dag_id, insert._dag_id]
    )


def test_cycle_detection_still_works():
    a = _select("silver", "A")
    b = _select("silver", "B")

    dependencies = {
        a: SQLDependencies(reads_from={"silver.b"}, writes_to="silver.a"),
        b: SQLDependencies(reads_from={"silver.a"}, writes_to="silver.b"),
    }

    builder = OperationDAGBuilder([a, b], dependencies, settings=None)
    builder.build_dag()

    with pytest.raises(ValueError, match="Circular dependency"):
        builder.validate_dag()


def test_operation_reading_its_own_output_is_not_self_dependent():
    op = _select("silver", "Fact")
    dependencies = {
        op: SQLDependencies(reads_from={"silver.fact"}, writes_to="silver.fact")
    }

    builder = OperationDAGBuilder([op], dependencies, settings=None)
    dag = builder.build_dag()

    assert dag.get_dependencies(op._dag_id) == []
