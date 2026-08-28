"""SQL Dependency Analyzer for automatic DAG generation.

This module provides SQL parsing and dependency extraction capabilities
using SQLGlot to automatically detect table dependencies in SQL queries.
It enables automatic DAG generation for ETL sequencers without requiring
manual specification of dependencies.

The analyzer extracts:
- Source tables (FROM, JOIN clauses)
- Target tables (INSERT, UPDATE, MERGE operations)
- medalflow dependencies within queries
- Cross-query dependencies for DAG building

Example:
    >>> analyzer = SQLDependencyAnalyzer()
    >>> deps = analyzer.extract_dependencies(
    ...     "INSERT INTO silver.customers SELECT * FROM bronze.raw_customers"
    ... )
    >>> print(deps)
    {
        'reads_from': {'bronze.raw_customers'},
        'writes_to': 'silver.customers',
        'query_type': 'INSERT'
    }
"""

from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp

from medalflow.constants.sql import QueryType
from medalflow.logging import get_logger
from medalflow.observability.context import sanitize_extras
from medalflow.operations import BaseOperation
from medalflow.types.metadata import SQLDependencies

if TYPE_CHECKING:
    from medalflow.settings import MedalflowSettings

logger = get_logger(__name__)

# Statements that produce a table the DAG can hang an edge on. `exp.Drop` is
# deliberately absent: a DROP neither produces nor consumes rows, and reporting
# its target as either invents an edge.
_WRITE_EXPRESSIONS = (exp.Create, exp.Insert, exp.Update, exp.Merge)

# SQL Server's system catalog. Guarded DDL probes it (`sys.external_tables`,
# `sys.schemas`); it is never a data dependency.
_CATALOG_SCHEMA = "sys"


class SQLDependencyAnalyzer:
    """Analyzes SQL queries to extract table dependencies using SQLGlot parser.

    This analyzer uses SQLGlot's AST (Abstract Syntax Tree) parsing to accurately
    extract table dependencies from SQL queries. It handles complex SQL patterns
    including CTEs, subqueries, joins, and various SQL dialects.

    Attributes:
        dialect: SQL dialect to use for parsing (default: tsql for Synapse)
        fallback_on_error: Whether to use regex fallback on parse errors

    Example:
        >>> analyzer = SQLDependencyAnalyzer(dialect="tsql")
        >>> sql = '''
        ...     WITH cte AS (SELECT * FROM staging.temp)
        ...     INSERT INTO silver.fact_sales
        ...     SELECT * FROM cte JOIN dim.products p ON cte.product_id = p.id
        ... '''
        >>> deps = analyzer.extract_dependencies(sql)
        >>> print(deps['reads_from'])
        {'staging.temp', 'dim.products'}
        >>> print(deps['writes_to'])
        'silver.fact_sales'
    """

    def __init__(self, settings: "MedalflowSettings"):
        """Initialize the SQL dependency analyzer.

        Args:
            dialect: SQL dialect for parsing (tsql, spark, snowflake, etc.)
            fallback_on_error: Use regex fallback if SQLGlot parsing fails
        """
        self.settings = settings
        self.dialect = settings.compute.active_config.dialect
        self.table_prefix = settings.table_prefix

    def extract_dependencies(self, sql: str) -> SQLDependencies:
        """Extract source and target tables from SQL query.

        Args:
            sql: SQL query string to analyze

        Returns:
            SQLDependencies object containing:
                - reads_from: Set of source table names
                - writes_to: Target table name (if DML operation)

        Example:
            >>> deps = analyzer.extract_dependencies(
            ...     "INSERT INTO t1 SELECT * FROM t2 JOIN t3"
            ... )
            >>> deps['reads_from']
            {'t2', 't3'}
            >>> deps['writes_to']
            't1'
        """
        if not sql or not sql.strip():
            raise ValueError("SQL query must be a non-empty string.")

        reads_from: set[str] = set()
        writes_to: str | None = None

        # Guarded DDL really is several statements, so parse the plural; the
        # first statement is not necessarily the interesting one.
        for statement in sqlglot.parse(sql, dialect=self.dialect):
            if statement is None:
                continue

            statement_reads, statement_writes = self._analyze_statement(statement)
            reads_from |= statement_reads
            if writes_to is None:
                writes_to = statement_writes

        return SQLDependencies(reads_from=reads_from, writes_to=writes_to)

    def _analyze_statement(self, statement: "exp.Expression") -> tuple[set[str], str | None]:
        """Split one parsed statement into the tables it reads and the one it writes.

        The write expression is searched for anywhere in the tree instead of
        being assumed to be the root: ``recreate=True`` renders the CETAS
        inside an ``IF EXISTS (...) DROP EXTERNAL TABLE ...;`` guard, which
        sqlglot parses as a single ``IfBlock`` whose ``this`` is the guard
        condition, not the target. Narrowing the read scope to the write
        expression is also what keeps the guard's catalog probe and its own
        DROP target out of ``reads_from``.

        Args:
            statement: One parsed statement

        Returns:
            ``(reads_from, writes_to)`` for that statement
        """
        write = next(statement.find_all(*_WRITE_EXPRESSIONS), None)

        if write is None:
            if isinstance(statement, exp.Drop):
                return set(), None
            return self._source_tables(statement, target=None), None

        target = self._target_table(write)
        writes_to = None if target is None else self._qualified_name(self._table_parts(target))
        return self._source_tables(write, target=target), writes_to

    @staticmethod
    def _target_table(write: "exp.Expression") -> "exp.Table | None":
        """Return the table a write expression targets, or None if it has none.

        ``CREATE TABLE t (a INT)`` and ``INSERT INTO t (a)`` both wrap the table
        in a ``Schema`` node; ``CREATE SCHEMA`` targets no table at all.
        """
        target = write.this
        if isinstance(target, exp.Schema):
            target = target.this
        return target if isinstance(target, exp.Table) else None

    def _source_tables(
        self, scope: "exp.Expression", target: "exp.Table | None" = None
    ) -> set[str]:
        """Extract the source tables read within ``scope``.

        Args:
            scope: Subtree to search -- the write expression when there is one
            target: The write target, which is not a source of itself

        Returns:
            Set of qualified, lowercase table names
        """
        ctes = self._extract_ctes_sqlglot(scope)
        tables: set[str] = set()

        for table in scope.find_all(exp.Table):
            if table is target or table.db.lower() == _CATALOG_SCHEMA:
                continue

            full_table_name = self._qualified_name(self._table_parts(table))
            if self._is_cte(full_table_name, ctes):
                continue

            tables.add(full_table_name)
        return tables

    def _extract_ctes_sqlglot(self, ast: "exp.Expression") -> set[str]:
        """Extract CTE names from SQLGlot AST.

        Args:
            ast: SQLGlot expression tree

        Returns:
            List of CTE names defined in the query
        """
        return {cte.alias.lower() for cte in ast.find_all(exp.CTE) if getattr(cte, "alias", None)}

    def _is_cte(self, table_name: str, cte_names: set[str]) -> bool:
        """Check if table name is a CTE or temporary construct.

        Args:
            table_name: Table name to check
            cte_names: Set of CTE names in query

        Returns:
            True if table is CTE or temporary
        """
        if not table_name:
            return True

        # Check if it's a CTE
        table_parts = table_name.split(".")
        if table_parts[-1] in cte_names:
            return True

        return False

    def _table_parts(self, table: exp.Table) -> dict:
        """Convert a table object to its string representation.

        Args:
            table: Table object from SQLGlot AST

        Returns:
            String representation of the table name (without alias)
        """
        return {"database": table.catalog, "schema": table.db, "table": table.name}

    @staticmethod
    def _qualified_name(table_parts: dict) -> str:
        """Join table parts into one lowercase ``[database.]schema.table`` name.

        This is the single naming convention for both ``reads_from`` and
        ``writes_to``; the DAG builder matches operations on exactly these
        strings, so producers must not diverge from it.
        """
        return ".".join(part for part in table_parts.values() if part).lower()

    def analyze_operations(
        self, operations: list[BaseOperation]
    ) -> dict[BaseOperation, SQLDependencies]:
        """Analyze dependencies for a list of database operations.

        This method extracts SQL from operations and analyzes their
        dependencies to understand data flow between operations.

        Args:
            operations: List of database operations to analyze

        Returns:
            Dictionary mapping operations to their SQL dependencies

        Example:
            >>> ops = [CreateTable(...), Insert(...), Update(...)]
            >>> deps = analyzer.analyze_operations(ops)
            >>> print(deps[ops[1]].reads_from)
            {'source_table'}
        """
        from medalflow.query_builder.factory import create_query_builder

        operation_dependencies = {}
        query_builder = create_query_builder()

        for operation in operations:
            try:
                # Extract SQL from operation using query builder
                sql = query_builder.build_query(operation)

                # Analyze dependencies directly from SQL
                deps = self.extract_dependencies(sql)

                # Store dependencies for this operation
                operation_dependencies[operation] = deps

                logger.debug(
                    "dependency.analyzer.operation_analyzed",
                    extra=sanitize_extras(
                        {
                            "operation_type": str(operation.operation_type),
                            "schema": getattr(operation, "schema_name", None)
                            or getattr(operation, "schema", None),
                            "object": operation.object_name,
                            "sources": list(deps.reads_from),
                            "target": deps.writes_to,
                        }
                    ),
                )

            except Exception as e:
                logger.warning(
                    "dependency.analyzer.operation_failed",
                    extra=sanitize_extras(
                        {
                            "operation": repr(operation),
                            "error": str(e),
                        }
                    ),
                    exc_info=True,
                )
                # Store minimal dependencies on error
                # Use fully qualified name as fallback for write operations
                operation_dependencies[operation] = SQLDependencies(
                    reads_from=set(),
                    writes_to=(
                        f"{operation.schema_name}.{operation.object_name}".lower()
                        if operation.operation_type
                        in [
                            QueryType.CREATE_TABLE,
                            QueryType.INSERT,
                            QueryType.UPDATE,
                            QueryType.MERGE,
                            QueryType.DELETE,
                        ]
                        else None
                    ),
                )

        return operation_dependencies
