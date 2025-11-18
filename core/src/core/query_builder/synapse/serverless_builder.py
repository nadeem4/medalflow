"""Synapse Serverless SQL pool query builder implementation."""

from dataclasses import dataclass
from typing import List, Optional, TYPE_CHECKING

# Import operation types from Layer 1
from core.operations import (
    CreateTable, DropTable,
    Insert, Update, Delete, Merge, Copy,
    CreateOrAlterView, DropView,
    CreateStatistics, CreateSchema, DropSchema,
    Select, ExecuteSQL
)

# Import from Layer 1 and Layer 0
from core.query_builder.base import BaseQueryBuilder
from core.protocols.operations import ColumnDefinition
from core.settings import _Settings



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
    
    def __init__(self, settings: _Settings):
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
            location = operation.location or self._generate_location(operation.schema_name, operation.object_name)
            file_format = self.csv_file_format_name if operation.file_format == "csv" else self.parquet_file_format_name
            
            sql = f"""CREATE EXTERNAL TABLE {full_name}
WITH (
    DATA_SOURCE = {self.proc_data_source_name},
    LOCATION = '{location}',
    FILE_FORMAT = {file_format}
)
AS {operation.select_query}"""
            
        # External table over existing data
        elif operation.location:
            if not operation.columns:
                raise ValueError(f"Columns required for external table over existing data: {operation.object_name}")
            
            columns_sql = self.format_column_definitions(operation.columns)
            file_format = self.csv_file_format_name if operation.file_format == "csv" else self.parquet_file_format_name
            
            sql = f"""CREATE EXTERNAL TABLE {full_name} (
    {columns_sql}
)
WITH (
    DATA_SOURCE = {self.proc_data_source_name},
    LOCATION = '{operation.location}',
    FILE_FORMAT = {file_format}
)"""
            
        # CREATE TABLE with columns (creates external table in Synapse Serverless)
        elif operation.columns:
            location = self._generate_location(operation.schema_name, operation.object_name)
            columns_sql = self.format_column_definitions(operation.columns)
            file_format = self.csv_file_format_name if operation.file_format == "csv" else self.parquet_file_format_name
            
            sql = f"""CREATE EXTERNAL TABLE {full_name} (
    {columns_sql}
)
WITH (
    DATA_SOURCE = {self.proc_data_source_name},
    LOCATION = '{location}',
    FILE_FORMAT = {file_format}
)"""
            
        else:
            raise ValueError(f"CreateTable requires either select_query, location, or columns: {operation.object_name}")
        
        if operation.recreate:
            drop_sql = f"IF EXISTS (SELECT * FROM sys.external_tables WHERE object_id = OBJECT_ID('{full_name}'))\n"
            drop_sql += f"    DROP EXTERNAL TABLE {full_name};\n"
            return drop_sql + sql
        
        return sql
    
    def _build_drop_table(self, operation: DropTable) -> str:
        """Build DROP TABLE statement."""
        full_name = self.fully_qualified_name(operation.schema_name, operation.object_name)
        
        if operation.if_exists:
            sql = f"IF EXISTS (SELECT * FROM sys.external_tables WHERE object_id = OBJECT_ID('{full_name}'))\n"
            sql += f"    DROP EXTERNAL TABLE {full_name}"
            return sql
        else:
            return f"DROP EXTERNAL TABLE {full_name}"
    
    def _build_insert(self, operation: Insert) -> str:
        """Build INSERT statement.
        
        Note: Cannot INSERT into external tables in Synapse Serverless.
        Must use CETAS to create new tables with data.
        """
        raise NotImplementedError("Cannot INSERT into external tables in Synapse Serverless. Use CREATE EXTERNAL TABLE AS SELECT instead.")
    
    def _build_update(self, operation: Update) -> str:
        """Build UPDATE statement.
        
        Note: Cannot UPDATE external tables in Synapse Serverless.
        """
        raise NotImplementedError("Cannot UPDATE external tables in Synapse Serverless. Data is read-only.")
    
    def _build_delete(self, operation: Delete) -> str:
        """Build DELETE statement.
        
        Note: Cannot DELETE from external tables in Synapse Serverless.
        """
        raise NotImplementedError("Cannot DELETE from external tables in Synapse Serverless. Data is read-only.")
    
    def _build_merge(self, operation: Merge) -> str:
        """Build MERGE statement.
        
        Note: MERGE not supported for external tables in Synapse Serverless.
        """
        raise NotImplementedError("MERGE not supported for external tables in Synapse Serverless.")
    
    def _build_copy(self, operation: Copy) -> str:
        """Build COPY statement.
        
        Synapse uses OPENROWSET for copying data from external sources.
        """
        raise NotImplementedError("COPY operation not implemented. Use OPENROWSET for ad-hoc queries.")
    
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
        
        return f"{create_clause} {full_name}{columns_list}{with_clause} AS\n{operation.select_query}"
    
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
            return f"""IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{operation.schema_name}')
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
            return f"""IF EXISTS (SELECT * FROM sys.schemas WHERE name = '{operation.schema_name}')
BEGIN
    DROP SCHEMA {schema_name}
END"""
        else:
            return f"DROP SCHEMA {schema_name}"
    
    def _build_select(self, operation: Select) -> str:
        """Build SELECT statement."""
        full_name = self.fully_qualified_name(operation.schema_name, operation.object_name)
        
        # Build SELECT clause
        if operation.distinct:
            select_clause = "SELECT DISTINCT"
        else:
            select_clause = "SELECT"
        
        # Column list
        if operation.columns:
            columns = self.format_column_list(operation.columns)
        else:
            columns = "*"
        
        # Build FROM clause
        sql = f"{select_clause} {columns} FROM {full_name}"
        
        # Add JOIN clause
        if operation.join_clause:
            sql += f" {operation.join_clause}"
        
        # Add WHERE clause
        if operation.where_clause:
            sql += f" WHERE {operation.where_clause}"
        
        # Add GROUP BY
        if operation.group_by:
            group_columns = self.format_column_list(operation.group_by)
            sql += f" GROUP BY {group_columns}"
            
            # Add HAVING clause (only valid with GROUP BY)
            if operation.having_clause:
                sql += f" HAVING {operation.having_clause}"
        
        # Add ORDER BY
        if operation.order_by:
            order_columns = ", ".join(operation.order_by)
            sql += f" ORDER BY {order_columns}"
        
        # Add LIMIT/OFFSET (T-SQL uses OFFSET...FETCH)
        if operation.limit is not None:
            if operation.offset is not None:
                sql += f" OFFSET {operation.offset} ROWS FETCH NEXT {operation.limit} ROWS ONLY"
            else:
                # If no offset, use TOP for better performance
                # Rebuild query with TOP
                if operation.distinct:
                    select_clause = f"SELECT DISTINCT TOP {operation.limit}"
                else:
                    select_clause = f"SELECT TOP {operation.limit}"
                sql = f"{select_clause} {columns} FROM {full_name}"
                if operation.join_clause:
                    sql += f" {operation.join_clause}"
                if operation.where_clause:
                    sql += f" WHERE {operation.where_clause}"
                if operation.group_by:
                    group_columns = self.format_column_list(operation.group_by)
                    sql += f" GROUP BY {group_columns}"
                    if operation.having_clause:
                        sql += f" HAVING {operation.having_clause}"
                if operation.order_by:
                    order_columns = ", ".join(operation.order_by)
                    sql += f" ORDER BY {order_columns}"
        elif operation.offset is not None:
            # OFFSET without LIMIT requires a large number for FETCH
            sql += f" OFFSET {operation.offset} ROWS"
        
        return sql
    
    def _build_execute_sql(self, operation: ExecuteSQL) -> str:
        """Build/validate arbitrary SQL statement."""
        # Basic validation to prevent obvious injection attempts
        sql = operation.sql.strip()
        
        # Check for dangerous patterns
        dangerous_patterns = [
            r'xp_cmdshell',
            r'sp_configure',
            r'sp_addextendedproc',
            r'sp_execute_external_script'
        ]
        
        import re
        sql_upper = sql.upper()
        for pattern in dangerous_patterns:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                raise ValueError(f"Potentially dangerous SQL pattern detected: {pattern}")
        
        # For SELECT queries with limit, wrap in subquery
        if operation.returns_results and operation.limit is not None:
            # Check if it's a SELECT statement
            if sql_upper.startswith('SELECT'):
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
    
    def format_column_definitions(self, columns: List[ColumnDefinition]) -> str:
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
    
    def build_is_external_table_query(self, schema: str, object_name: str) -> str:
        """Build query to check if a table is an external table.
        
        Args:
            schema: Schema name
            object_name: Table name (without prefix)
            
        Returns:
            SQL query to check if the table is external
        """
        # Get the fully qualified name with proper prefix handling
        full_name = self.fully_qualified_name(schema, object_name)
        # Extract schema and prefixed table name from the fully qualified name
        schema_part, table_part = full_name.split('.')
        
        # Use parameterized approach by escaping single quotes
        schema_escaped = schema_part.replace("'", "''")
        table_escaped = table_part.replace("'", "''")
        
        return f"""
        SELECT 1 FROM sys.external_tables et
        JOIN sys.schemas s ON et.schema_id = s.schema_id
        WHERE s.name = '{schema_escaped}' AND et.name = '{table_escaped}'
        """
    
    def build_get_external_table_location_query(self, schema: str, object_name: str) -> str:
        """Build query to get the ADLS location of an external table.
        
        Args:
            schema: Schema name
            object_name: Table name (without prefix)
            
        Returns:
            SQL query to get the external table location
        """
        # Get the fully qualified name with proper prefix handling
        full_name = self.fully_qualified_name(schema, object_name)
        # Extract schema and prefixed table name from the fully qualified name
        schema_part, table_part = full_name.split('.')
        
        # Use parameterized approach by escaping single quotes
        schema_escaped = schema_part.replace("'", "''")
        table_escaped = table_part.replace("'", "''")
        
        return f"""
        SELECT eds.location + '/' + et.location AS full_location
        FROM sys.external_tables et
        JOIN sys.schemas s ON et.schema_id = s.schema_id
        JOIN sys.external_data_sources eds ON et.data_source_id = eds.data_source_id
        WHERE s.name = '{schema_escaped}' AND et.name = '{table_escaped}'
        """