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
from sqlglot.errors import ParseError
from sqlglot.tokens import TokenType

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

# Operation types that produce or mutate the table the DAG hangs an edge on.
# `analyze_operations` reads the target off the operation rather than out of
# generated SQL, so this is the list that decides who is a writer.
#
# Included: CREATE_TABLE and CREATE_OR_ALTER_VIEW produce the object; INSERT,
# UPDATE, DELETE, MERGE and COPY change the rows in it, so a reader must be
# ordered after them.
#
# Excluded, deliberately:
#   SELECT                          reads only.
#   DROP_TABLE/DROP_VIEW/DROP_SCHEMA  remove the object. A DROP produces no
#                                   rows for anyone to read, so reporting its
#                                   target invents an edge -- the same reason
#                                   `exp.Drop` is absent from
#                                   `_WRITE_EXPRESSIONS` above.
#   CREATE_SCHEMA                   its `object_name` is a schema, not a table.
#   CREATE_STATISTICS/DROP_STATISTICS  attach optimiser metadata to a table
#                                   that already exists; they produce nothing.
#   TRUNCATE/ALTER                  no operation class emits them today.
#   EXECUTE_SQL                     carries arbitrary SQL, so the operation
#                                   does not know its own target; that one is
#                                   left to the SQL, which is the only thing
#                                   that does know.
#   UNKNOWN                         says so.
_WRITER_QUERY_TYPES = frozenset(
    {
        QueryType.CREATE_TABLE,
        QueryType.CREATE_OR_ALTER_VIEW,
        QueryType.INSERT,
        QueryType.UPDATE,
        QueryType.DELETE,
        QueryType.MERGE,
        QueryType.COPY,
    }
)


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
        self.skip_prefix_on_schema = settings.compute.active_config.skip_prefix_on_schema

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
        for statement in self._parse_statements(sql):
            statement_reads, statement_writes = self._analyze_statement(statement)
            reads_from |= statement_reads
            if writes_to is None:
                writes_to = statement_writes

        return SQLDependencies(reads_from=reads_from, writes_to=writes_to)

    def _parse_statements(self, sql: str) -> list["exp.Expression"]:
        """Parse ``sql`` a statement at a time, isolating the unreadable ones.

        ``sqlglot.parse`` is all-or-nothing, and in guarded DDL the fragile
        statement is the guard, not the payload: ``IF EXISTS (...) DROP
        EXTERNAL TABLE x;`` is a ``Command`` on sqlglot 30 but a hard
        ``ParseError`` on 23 (the version the lock resolves), which would take
        the CETAS that follows -- and every edge in it -- down with it.
        Parsing per statement means one statement sqlglot cannot read costs
        only itself.

        A statement is used whole or not at all; nothing is salvaged from a
        partial parse, so no half-built tree can invent a dependency.

        Raises:
            ParseError: if not one statement could be read. The analyzer then
                knows nothing whatsoever about this SQL, and an empty result
                would be indistinguishable from a real answer of "no
                dependencies".
        """
        statements: list[exp.Expression] = []
        unreadable: list[tuple[str, ParseError]] = []

        for statement_sql in self._split_statements(sql):
            try:
                statement = sqlglot.parse_one(statement_sql, dialect=self.dialect)
            except ParseError as error:
                unreadable.append((statement_sql, error))
                continue
            if statement is not None:
                statements.append(statement)

        if unreadable and not statements:
            raise unreadable[0][1]

        for statement_sql, error in unreadable:
            logger.warning(
                "dependency.analyzer.statement_unreadable",
                extra=sanitize_extras(
                    {
                        "dialect": self.dialect,
                        "statement": statement_sql,
                        "error": str(error),
                        "impact": (
                            "statement skipped; any dependency it declares is "
                            "missing from the returned dependencies"
                        ),
                    }
                ),
            )

        return statements

    def _split_statements(self, sql: str) -> list[str]:
        """Split ``sql`` on its statement terminators.

        The tokenizer does the splitting because it is the only thing that
        knows a ``;`` inside a string literal or a comment does not end a
        statement.
        """
        statements: list[str] = []
        start = 0

        for token in sqlglot.tokenize(sql, read=self.dialect):
            if token.token_type is TokenType.SEMICOLON:
                statements.append(sql[start : token.start])
                start = token.end + 1
        statements.append(sql[start:])

        return [statement for statement in statements if statement.strip()]

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

    def _qualified_name(self, table_parts: dict) -> str:
        """Join table parts into one lowercase ``[database.]schema.table`` name.

        This is the single naming convention for both ``reads_from`` and
        ``writes_to``; the DAG builder matches operations on exactly these
        strings, so producers must not diverge from it.

        The name is *logical*: the deployment table prefix is stripped, so
        generated ``[silver].[fin_DimCustomer]`` and a model author's
        ``silver.DimCustomer`` are the same table. See
        docs/adr/000-table-prefix-is-deployment-detail.md.
        """
        schema = table_parts.get("schema") or ""
        table = table_parts.get("table") or ""

        if self._is_prefixed_schema(schema) and table.lower().startswith(self.table_prefix.lower()):
            table = table[len(self.table_prefix) :]

        parts = (table_parts.get("database"), schema, table)
        return ".".join(part for part in parts if part).lower()

    def _is_prefixed_schema(self, schema: str) -> bool:
        """Whether the query builder would prefix a table living in this schema.

        Mirrors ``BaseQueryBuilder.fully_qualified_name``: ``skip_prefix_on_schema``
        makes the prefix conditional, so it cannot be stripped blindly. An
        unqualified name has no schema to judge by, so it is left alone.
        """
        if not self.table_prefix or not schema:
            return False

        # Compared exactly as `fully_qualified_name` does, mis-cased config and
        # all: the analyzer has to model what the builder actually emitted, not
        # what it arguably should have.
        return schema.lower() not in self.skip_prefix_on_schema

    def _declared_target(self, operation: BaseOperation) -> str | None:
        """The table an operation says it writes, or None if it writes none.

        The operation is the source of truth for its own target.
        ``CreateTable(schema_name="bronze", object_name="Customers")`` already
        knows what it produces, so re-deriving that by parsing SQL the query
        builder has just generated buys nothing and costs a dependency on
        every dialect quirk, DDL guard and sqlglot release.

        Normalised through ``_qualified_name`` so a declared target and a
        target read out of generated SQL are the same string: the declared
        name is already logical, and running it through the same helper keeps
        both sides on one convention (ADR 000).
        """
        if operation.operation_type not in _WRITER_QUERY_TYPES:
            return None

        return self._qualified_name(
            {
                "database": None,
                "schema": operation.schema_name,
                "table": operation.object_name,
            }
        )

    def analyze_operations(
        self, operations: list[BaseOperation]
    ) -> dict[BaseOperation, SQLDependencies]:
        """Analyze dependencies for a list of database operations.

        The two halves of the answer come from different places on purpose:

        - ``writes_to`` comes from the operation, which declares it. It cannot
          be lost to a parse failure.
        - ``reads_from`` comes from parsing the generated SQL, which is the
          only thing that knows it.

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

        operation_dependencies: dict[BaseOperation, SQLDependencies] = {}
        query_builder = create_query_builder()

        for operation in operations:
            writes_to = self._declared_target(operation)
            reads_from: set[str] = set()
            sql: str | None = None

            try:
                sql = query_builder.build_query(operation)
                deps = self.extract_dependencies(sql)
            except Exception as error:
                # Not raised: a plan is still buildable without an edge the
                # analyzer could not see -- the operation keeps its declared
                # target, so it is still a producer, and the DAG builder still
                # rejects cycles. Raising would turn a parser limitation on
                # framework-generated guard DDL (sqlglot 23 cannot read the
                # `CREATE SCHEMA` guard at all) into a hard planning failure
                # for a perfectly correct project. It is logged at ERROR, with
                # the operation and its SQL, so it cannot pass unnoticed.
                logger.error(
                    "dependency.analyzer.sql_not_analyzable",
                    extra=sanitize_extras(
                        {
                            "operation_type": str(operation.operation_type),
                            "schema": operation.schema_name,
                            "object": operation.object_name,
                            "declared_target": writes_to,
                            "sql": sql,
                            "error": str(error),
                            "impact": (
                                "sources could not be read from this SQL, so every "
                                "dependency edge into this operation is missing from "
                                "the DAG and it may be scheduled too early"
                            ),
                        }
                    ),
                    exc_info=True,
                )
            else:
                reads_from = deps.reads_from
                # Only operations that declare no target of their own fall
                # back to the parsed one -- EXECUTE_SQL, whose target lives in
                # SQL the operation never inspects.
                if writes_to is None:
                    writes_to = deps.writes_to

            operation_dependencies[operation] = SQLDependencies(
                reads_from=reads_from, writes_to=writes_to
            )

            logger.debug(
                "dependency.analyzer.operation_analyzed",
                extra=sanitize_extras(
                    {
                        "operation_type": str(operation.operation_type),
                        "schema": operation.schema_name,
                        "object": operation.object_name,
                        "sources": list(reads_from),
                        "target": writes_to,
                    }
                ),
            )

        return operation_dependencies
