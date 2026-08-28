"""Synapse Serverless SQL pool query builder implementation."""


# Import operation types from Layer 1
from medalflow.operations import (
    Copy,
    CreateOrAlterView,
    CreateSchema,
    CreateStatistics,
    CreateTable,
    Delete,
    DropSchema,
    DropTable,
    DropView,
    ExecuteSQL,
    Insert,
    Merge,
    Select,
    Update,
)
from medalflow.operations.columns import ColumnDefinition

# Import from Layer 1 and Layer 0
from medalflow.query_builder.base import BaseQueryBuilder
from medalflow.settings import MedalflowSettings


class SynapseServerlessQueryBuilder(BaseQueryBuilder):
    """Query builder for Synapse Serverless SQL pools.

    Generates T-SQL queries specific to Azure Synapse Analytics Serverless SQL pools,
    focusing on external tables, OPENROWSET, and data lake integration.
    This builder is optimized for serverless querying patterns where data remains
    in the data lake and is queried on-demand.

    Key Features:
        - External table creation with PolyBase
        - OPENROWSET for ad-hoc queries over data lake files
        - CETAS (Create External Table As Select)
        - Integration with ADLS Gen2
        - File format specifications (Parquet, CSV, Delta)
        - No data movement - queries data in-place
    """

    def __init__(self, settings: MedalflowSettings):
        """Initialize Synapse Serverless query builder.

        Args:
            synapse_config: Synapse-specific configuration
            table_prefix: Optional prefix to add to table names (e.g., 'sap_', 'oracle_')
        """
        super().__init__(settings)
        self.compute_settings = settings.compute.synapse

        # Store Synapse-specific configuration
        self.proc_data_source_name = self.compute_settings.processed_external_data_source_name
        self.raw_data_source_name = self.compute_settings.raw_external_data_source_name
        self.parquet_file_format_name = self.compute_settings.parquet_file_format
        self.csv_file_format_name = self.compute_settings.csv_file_format
        self.location_prefix = self.settings.full_path

    def _build_create_table(self, operation: CreateTable) -> str:
        """Build CREATE EXTERNAL TABLE statement for Synapse.

        Synapse Serverless always uses external tables that reference data in the lake.
        """
        full_name = self.fully_qualified_name(operation.schema_name, operation.object_name)

        # CETAS - Create External Table As Select
        if operation.select_query:
            location = operation.location or self._generate_location(
                operation.schema_name, operation.object_name
            )
            file_format = (
                self.csv_file_format_name
                if operation.file_format == "csv"
                else self.parquet_file_format_name
            )

            sql = f"""CREATE EXTERNAL TABLE {full_name}
WITH (
    DATA_SOURCE = {self.proc_data_source_name},
    LOCATION = {self.quote_string(location)},
    FILE_FORMAT = {file_format}
)
AS {operation.select_query}"""

        # External table over existing data
        elif operation.location:
            if not operation.columns:
                raise ValueError(
                    f"Columns required for external table over existing data: {operation.object_name}"
                )

            columns_sql = self.format_column_definitions(operation.columns)
            file_format = (
                self.csv_file_format_name
                if operation.file_format == "csv"
                else self.parquet_file_format_name
            )

            sql = f"""CREATE EXTERNAL TABLE {full_name} (
    {columns_sql}
)
WITH (
    DATA_SOURCE = {self.proc_data_source_name},
    LOCATION = {self.quote_string(operation.location)},
    FILE_FORMAT = {file_format}
)"""

        # CREATE TABLE with columns (creates external table in Synapse Serverless)
        elif operation.columns:
            location = self._generate_location(operation.schema_name, operation.object_name)
            columns_sql = self.format_column_definitions(operation.columns)
            file_format = (
                self.csv_file_format_name
                if operation.file_format == "csv"
                else self.parquet_file_format_name
            )

            sql = f"""CREATE EXTERNAL TABLE {full_name} (
    {columns_sql}
)
WITH (
    DATA_SOURCE = {self.proc_data_source_name},
    LOCATION = {self.quote_string(location)},
    FILE_FORMAT = {file_format}
)"""

        else:
            raise ValueError(
                f"CreateTable requires either select_query, location, or columns: {operation.object_name}"
            )

        if operation.recreate:
            drop_sql = f"IF EXISTS (SELECT * FROM sys.external_tables WHERE object_id = OBJECT_ID({self.quote_string(full_name)}))\n"
            drop_sql += f"    DROP EXTERNAL TABLE {full_name};\n"
            return drop_sql + sql

        return sql

    def _build_drop_table(self, operation: DropTable) -> str:
        """Build DROP TABLE statement."""
        full_name = self.fully_qualified_name(operation.schema_name, operation.object_name)

        if operation.if_exists:
            sql = f"IF EXISTS (SELECT * FROM sys.external_tables WHERE object_id = OBJECT_ID({self.quote_string(full_name)}))\n"
            sql += f"    DROP EXTERNAL TABLE {full_name}"
            return sql
        else:
            return f"DROP EXTERNAL TABLE {full_name}"

    def _build_insert(self, operation: Insert) -> str:
        """Build INSERT statement.

        Note: Cannot INSERT into external tables in Synapse Serverless.
        Must use CETAS to create new tables with data.
        """
        raise NotImplementedError(
            "Cannot INSERT into external tables in Synapse Serverless. Use CREATE EXTERNAL TABLE AS SELECT instead."
        )

    def _build_update(self, operation: Update) -> str:
        """Build UPDATE statement.

        Note: Cannot UPDATE external tables in Synapse Serverless.
        """
        raise NotImplementedError(
            "Cannot UPDATE external tables in Synapse Serverless. Data is read-only."
        )

    def _build_delete(self, operation: Delete) -> str:
        """Build DELETE statement.

        Note: Cannot DELETE from external tables in Synapse Serverless.
        """
        raise NotImplementedError(
            "Cannot DELETE from external tables in Synapse Serverless. Data is read-only."
        )

    def _build_merge(self, operation: Merge) -> str:
        """Build MERGE statement.

        Note: MERGE not supported for external tables in Synapse Serverless.
        """
        raise NotImplementedError("MERGE not supported for external tables in Synapse Serverless.")

    def _build_copy(self, operation: Copy) -> str:
        """Build COPY statement.

        Synapse uses OPENROWSET for copying data from external sources.
        """
        raise NotImplementedError(
            "COPY operation not implemented. Use OPENROWSET for ad-hoc queries."
        )

    def _build_create_or_alter_view(self, operation: CreateOrAlterView) -> str:
        """Build CREATE OR ALTER VIEW statement."""
        full_name = self.fully_qualified_name(operation.schema_name, operation.object_name)

        # CREATE OR ALTER VIEW for idempotency
        create_clause = "CREATE OR ALTER VIEW"

        # Add column list if provided
        if operation.columns:
            columns_list = f"({self.format_column_list(operation.columns)})"
        else:
            columns_list = ""

        # Add WITH SCHEMABINDING if requested
        with_clause = " WITH SCHEMABINDING" if operation.with_schemabinding else ""

        return (
            f"{create_clause} {full_name}{columns_list}{with_clause} AS\n{operation.select_query}"
        )

    def _build_drop_view(self, operation: DropView) -> str:
        """Build DROP VIEW statement."""
        full_name = self.fully_qualified_name(operation.schema_name, operation.object_name)

        if operation.if_exists:
            return f"DROP VIEW IF EXISTS {full_name}"
        else:
            return f"DROP VIEW {full_name}"

    def _build_create_statistics(self, operation: CreateStatistics) -> str:
        """Build CREATE STATISTICS statement for single-column statistics.

        Synapse Serverless only supports single-column statistics.
        The base query builder validates this constraint.
        """
        full_name = self.fully_qualified_name(operation.schema_name, operation.object_name)

        # Generate statistics name if not provided
        if operation.stats_name:
            stats_name = self.quote_identifier(operation.stats_name)
        else:
            # Auto-generate name based on table and single column
            column_name = operation.columns[0]  # Guaranteed to have exactly one column
            stats_name = self.quote_identifier(f"stat_{operation.object_name}_{column_name}")

        # Format the single column
        column = self.quote_identifier(operation.columns[0])

        # Build WITH clause
        if operation.with_fullscan:
            with_clause = " WITH FULLSCAN"
        elif operation.sample_percent:
            with_clause = f" WITH SAMPLE {operation.sample_percent} PERCENT"
        else:
            with_clause = ""

        return f"CREATE STATISTICS {stats_name} ON {full_name} ({column}){with_clause}"

    def _build_create_schema(self, operation: CreateSchema) -> str:
        """Build CREATE SCHEMA statement."""
        schema_name = self.quote_identifier(operation.schema_name)

        if operation.authorization:
            auth_clause = f" AUTHORIZATION {self.quote_identifier(operation.authorization)}"
        else:
            auth_clause = ""

        if operation.if_not_exists:
            # T-SQL doesn't have IF NOT EXISTS for schemas, need to check first
            return f"""IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = {self.quote_string(operation.schema_name)})
BEGIN
    CREATE SCHEMA {schema_name}{auth_clause}
END"""
        else:
            return f"CREATE SCHEMA {schema_name}{auth_clause}"

    def _build_drop_schema(self, operation: DropSchema) -> str:
        """Build DROP SCHEMA statement."""
        schema_name = self.quote_identifier(operation.schema_name)

        if operation.if_exists:
            # T-SQL doesn't have IF EXISTS for DROP SCHEMA, need to check first
            return f"""IF EXISTS (SELECT * FROM sys.schemas WHERE name = {self.quote_string(operation.schema_name)})
BEGIN
    DROP SCHEMA {schema_name}
END"""
        else:
            return f"DROP SCHEMA {schema_name}"

    def _build_select(self, operation: Select) -> str:
        """Build SELECT statement."""
        full_name = self.fully_qualified_name(operation.schema_name, operation.object_name)

        # TOP is only usable without an OFFSET, which needs OFFSET...FETCH.
        top = ""
        if operation.limit is not None and operation.offset is None:
            top = f" TOP {operation.limit}"

        select_clause = "SELECT DISTINCT" if operation.distinct else "SELECT"
        columns = self.format_column_list(operation.columns) if operation.columns else "*"

        sql = f"{select_clause}{top} {columns} FROM {full_name}"

        if operation.join_clause:
            sql += f" {operation.join_clause}"

        if operation.where_clause:
            sql += f" WHERE {operation.where_clause}"

        if operation.group_by:
            sql += f" GROUP BY {self.format_column_list(operation.group_by)}"

            # HAVING is only valid with GROUP BY
            if operation.having_clause:
                sql += f" HAVING {operation.having_clause}"

        if operation.order_by:
            sql += f" ORDER BY {self.format_order_by(operation.order_by)}"

        # T-SQL spells LIMIT/OFFSET as OFFSET...FETCH; without an offset the
        # TOP above already applied the limit.
        if operation.offset is not None:
            sql += f" OFFSET {operation.offset} ROWS"
            if operation.limit is not None:
                sql += f" FETCH NEXT {operation.limit} ROWS ONLY"

        return sql

    def _build_execute_sql(self, operation: ExecuteSQL) -> str:
        """Return the caller's SQL, optionally wrapped to apply a row limit.

        ``ExecuteSQL.sql`` is raw by design -- that is the point of the
        operation. It used to be screened against four ``xp_``/``sp_`` names
        before being returned verbatim anyway, which bought nothing against an
        infinite grammar and read as protection that was not there.
        """
        sql = str(operation.sql).strip()

        # For SELECT queries with limit, wrap in subquery
        if operation.returns_results and operation.limit is not None:
            if sql.upper().startswith("SELECT"):
                sql = f"SELECT TOP {operation.limit} * FROM ({sql}) AS limited_results"

        return sql

    # Helper methods
    def _generate_location(self, schema: str, table_name: str) -> str:
        """Generate ADLS location for a table."""
        # Validate identifiers
        self._validate_identifier(schema, "schema")
        self._validate_identifier(table_name, "table")

        # Use location prefix from settings
        return f"{self.location_prefix}/{schema}/{table_name}/"

    def format_column_definitions(self, columns: list[ColumnDefinition]) -> str:
        """Format column definitions for CREATE EXTERNAL TABLE.

        IMPORTANT: Synapse Serverless SQL external tables do not support:
        - PRIMARY KEY constraints
        - UNIQUE constraints
        - CHECK constraints
        - NOT NULL constraints
        - DEFAULT values

        All constraint definitions are stripped to prevent SQL errors.
        Data integrity must be maintained at the source file level.

        Args:
            columns: List of column definitions

        Returns:
            Column definitions for CREATE EXTERNAL TABLE (constraints stripped)
        """
        definitions = []
        for col in columns:
            # Only include column name and data type for external tables
            # All constraints are unsupported and must be excluded
            definition = f"{self.quote_identifier(col.name)} {col.data_type}"
            definitions.append(definition)

        return ",\n    ".join(definitions)
