"""Microsoft Fabric Data Warehouse query builder implementation."""

from typing import List, Optional, TYPE_CHECKING

# Import operation types from Layer 1
from core.operations import (
    BaseOperation,
    CreateTable, DropTable,
    Insert, Update, Delete, Merge, Copy,
    CreateOrAlterView, DropView,
    CreateStatistics, CreateSchema,
    DropSchema, Select, ExecuteSQL
)

# Import from Layer 1 and Layer 0
from core.query_builder.base import BaseQueryBuilder
from core.protocols.operations import ColumnDefinition
from core.settings import _Settings


class FabricWarehouseQueryBuilder(BaseQueryBuilder):
    """Query builder for Microsoft Fabric Data Warehouse.
    
    Generates T-SQL queries specific to Microsoft Fabric Data Warehouse,
    supporting managed Delta tables with automatic optimization features.
    
    Key Features:
        - Managed Delta tables (default storage format)
        - Direct OneLake integration
        - V-Order optimization for Power BI Direct Lake
        - Automatic file compaction and optimization
        - Native Delta Lake ACID transactions
        - Cross-workspace queries
        - Liquid clustering support
    
    Differences from Synapse:
        - Uses managed tables instead of external tables
        - Simplified USING DELTA syntax
        - Built-in Delta optimization features
        - No need for external data sources
        - Direct OneLake paths in queries
    """
    
    def __init__(self, settings: _Settings):
        """Initialize Fabric Warehouse query builder.
        
        Args:
            table_prefix: Optional prefix to add to table names (e.g., 'sap_', 'oracle_')
        """
        super().__init__(settings)
    
    def _build_create_table(self, operation: CreateTable) -> str:
        """Build CREATE TABLE statement for Fabric Warehouse.
        
        Fabric uses managed Delta tables by default.
        """
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        
        # CREATE TABLE AS SELECT (CTAS)
        if operation.select_query:
            sql = f"CREATE TABLE {full_name}"
            
            # Add USING DELTA for explicit Delta format
            sql += "\nUSING DELTA"
            
            # Add table properties if specified
            if operation.properties:
                props = ", ".join([f"'{k}' = '{v}'" for k, v in operation.properties.items()])
                sql += f"\nTBLPROPERTIES ({props})"
            
            sql += f"\nAS {operation.select_query}"
            
        # CREATE TABLE with columns
        elif operation.columns:
            columns_sql = self.format_column_definitions(operation.columns)
            
            sql = f"CREATE TABLE {full_name} (\n    {columns_sql}\n)"
            
            # Add USING DELTA (Fabric default, but explicit is better)
            sql += "\nUSING DELTA"
            
            # Add partitioning if specified
            if operation.partitions:
                partition_cols = ", ".join(operation.partitions)
                sql += f"\nPARTITIONED BY ({partition_cols})"
            
            # Add clustering if specified (Liquid Clustering)
            if hasattr(operation, 'cluster_by') and operation.cluster_by:
                cluster_cols = ", ".join(operation.cluster_by)
                sql += f"\nCLUSTER BY ({cluster_cols})"
            
            # Add table properties
            if operation.properties:
                props = ", ".join([f"'{k}' = '{v}'" for k, v in operation.properties.items()])
                sql += f"\nTBLPROPERTIES ({props})"
            
            # Add location if specified (for external data)
            if operation.location:
                sql += f"\nLOCATION '{operation.location}'"
                
        else:
            raise ValueError(f"CreateTable requires either select_query or columns: {operation.object_name}")
        
        # Handle recreate logic
        if operation.recreate:
            # Drop table first if recreate is True
            drop_sql = f"DROP TABLE IF EXISTS {full_name};\n"
            return drop_sql + sql
        else:
            # Only create if not exists when recreate is False
            sql = sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
            return sql
    
    def _build_drop_table(self, operation: DropTable) -> str:
        """Build DROP TABLE statement."""
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        
        if operation.if_exists:
            return f"DROP TABLE IF EXISTS {full_name}"
        else:
            return f"DROP TABLE {full_name}"
    
    def _build_alter_table(self, operation: BaseOperation) -> str:  # AlterTable not yet defined
        """Build ALTER TABLE statement.
        
        Fabric supports full ALTER TABLE operations on managed tables.
        """
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        
        if operation.add_columns:
            columns_sql = self.format_column_definitions(operation.add_columns)
            return f"ALTER TABLE {full_name} ADD COLUMNS ({columns_sql})"
        
        elif operation.drop_columns:
            # Note: Column drop requires Delta table property 'delta.columnMapping.mode' = 'name'
            columns = ", ".join([self.quote_identifier(col) for col in operation.drop_columns])
            return f"ALTER TABLE {full_name} DROP COLUMNS ({columns})"
        
        elif operation.rename_column:
            old_name = self.quote_identifier(operation.rename_column[0])
            new_name = self.quote_identifier(operation.rename_column[1])
            return f"ALTER TABLE {full_name} RENAME COLUMN {old_name} TO {new_name}"
        
        elif operation.rename_to:
            new_name = self.fully_qualified_name(operation.schema, operation.rename_to)
            return f"ALTER TABLE {full_name} RENAME TO {new_name}"
        
        elif operation.set_properties:
            props = ", ".join([f"'{k}' = '{v}'" for k, v in operation.set_properties.items()])
            return f"ALTER TABLE {full_name} SET TBLPROPERTIES ({props})"
        
        elif operation.unset_properties:
            props = ", ".join([f"'{prop}'" for prop in operation.unset_properties])
            return f"ALTER TABLE {full_name} UNSET TBLPROPERTIES ({props})"
        
        else:
            raise ValueError(f"No ALTER operation specified for table {operation.object_name}")
    
    def _build_truncate_table(self, operation: BaseOperation) -> str:  # TruncateTable not yet defined
        """Build TRUNCATE TABLE statement.
        
        Fabric supports TRUNCATE for managed Delta tables.
        """
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        return f"TRUNCATE TABLE {full_name}"
    
    def _build_insert(self, operation: Insert) -> str:
        """Build INSERT statement.
        
        Fabric supports full INSERT operations on managed tables.
        """
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        
        if operation.source_query:
            # INSERT INTO ... SELECT
            sql = f"INSERT INTO {full_name}"
            
            if operation.columns:
                columns = ", ".join([self.quote_identifier(col) for col in operation.columns])
                sql += f" ({columns})"
            
            if operation.mode == "overwrite":
                sql = sql.replace("INSERT INTO", "INSERT OVERWRITE")
            
            sql += f"\n{operation.source_query}"
            
        elif operation.values:
            # INSERT INTO ... VALUES
            sql = f"INSERT INTO {full_name}"
            
            if operation.columns:
                columns = ", ".join([self.quote_identifier(col) for col in operation.columns])
                sql += f" ({columns})"
            
            sql += f"\nVALUES {operation.values}"
            
        else:
            raise ValueError(f"Insert requires either source_query or values: {operation.object_name}")
        
        return sql
    
    def _build_update(self, operation: Update) -> str:
        """Build UPDATE statement.
        
        Fabric supports UPDATE on Delta tables.
        """
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        
        sql = f"UPDATE {full_name}"
        
        if operation.set_columns:
            set_clause = ", ".join([f"{self.quote_identifier(col)} = {val}" 
                                   for col, val in operation.set_columns.items()])
            sql += f"\nSET {set_clause}"
        else:
            raise ValueError(f"Update requires set_columns: {operation.object_name}")
        
        if operation.where_clause:
            sql += f"\nWHERE {operation.where_clause}"
        
        return sql
    
    def _build_delete(self, operation: Delete) -> str:
        """Build DELETE statement.
        
        Fabric supports DELETE on Delta tables.
        """
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        
        sql = f"DELETE FROM {full_name}"
        
        if operation.where_clause:
            sql += f"\nWHERE {operation.where_condition}"
        
        return sql
    
    def _build_merge(self, operation: Merge) -> str:
        """Build MERGE statement.
        
        Fabric supports full MERGE operations on Delta tables.
        """
        target = self.fully_qualified_name(operation.schema, operation.object_name)
        
        sql = f"MERGE INTO {target} AS target"
        sql += f"\nUSING ({operation.source_query}) AS source"
        sql += f"\nON {operation.merge_condition}"
        
        # WHEN MATCHED
        if operation.when_matched_update:
            sql += "\nWHEN MATCHED"
            # when_matched_delete is a condition string for when to delete
            # This should be a separate WHEN MATCHED clause
            
            set_clause = ", ".join([f"target.{self.quote_identifier(col)} = {val}" 
                                   for col, val in operation.when_matched_update.items()])
            sql += f" THEN UPDATE SET {set_clause}"
        
        if operation.when_matched_delete:
            sql += "\nWHEN MATCHED"
            # when_matched_delete contains the condition for delete
            sql += f" AND {operation.when_matched_delete}"
            sql += " THEN DELETE"
        
        # WHEN NOT MATCHED (INSERT)
        if operation.when_not_matched_insert:
            sql += "\nWHEN NOT MATCHED"
            if hasattr(operation, 'not_matched_condition') and operation.not_matched_condition:
                sql += f" AND {operation.not_matched_condition}"
            
            if isinstance(operation.when_not_matched_insert, dict):
                columns = list(operation.when_not_matched_insert.keys())
                values = list(operation.when_not_matched_insert.values())
            else:
                columns = operation.when_not_matched_insert
                values = [f"source.{self.quote_identifier(col)}" for col in columns]
            
            columns_str = ", ".join([self.quote_identifier(col) for col in columns])
            values_str = ", ".join(values)
            sql += f" THEN INSERT ({columns_str}) VALUES ({values_str})"
        
        return sql
    
    def _build_copy(self, operation: Copy) -> str:
        """Build COPY INTO statement.
        
        Fabric supports COPY INTO for loading external data.
        """
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        
        sql = f"COPY INTO {full_name}"
        
        # Copy doesn't have columns field - it copies all columns from source
        
        sql += f"\nFROM '{operation.source_path}'"
        
        # Add file format options
        if operation.file_format:
            sql += f"\nFILEFORMAT = {operation.file_format.upper()}"
        
        # Add additional options
        if operation.copy_options:
            for key, value in operation.copy_options.items():
                sql += f"\n{key} = {value}"
        
        return sql
    
    def _build_create_or_alter_view(self, operation: CreateOrAlterView) -> str:
        """Build CREATE OR ALTER VIEW statement."""
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        
        sql = "CREATE"
        
        if operation.or_replace:
            sql += " OR REPLACE"
        
        sql += f" VIEW {full_name}"
        
        if operation.columns:
            columns = ", ".join([self.quote_identifier(col) for col in operation.columns])
            sql += f" ({columns})"
        
        sql += f"\nAS {operation.select_query}"
        
        return sql
    
    def _build_drop_view(self, operation: DropView) -> str:
        """Build DROP VIEW statement."""
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        
        if operation.if_exists:
            return f"DROP VIEW IF EXISTS {full_name}"
        else:
            return f"DROP VIEW {full_name}"
    
    def _build_create_statistics(self, operation: CreateStatistics) -> str:
        """Build CREATE STATISTICS statement for single-column statistics.
        
        Note: Fabric automatically manages statistics for Delta tables.
        Manual statistics creation is typically not needed.
        Fabric only supports single-column histogram statistics.
        """
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        
        # Generate statistics name if not provided
        if operation.stats_name:
            stats_name = self.quote_identifier(operation.stats_name)
        else:
            # Auto-generate name based on table and single column
            column_name = operation.columns[0]  # Guaranteed to have exactly one column
            stats_name = self.quote_identifier(f"stat_{operation.object_name}_{column_name}")
        
        # Format the single column (guaranteed by base validation)
        column = self.quote_identifier(operation.columns[0])
        
        sql = f"CREATE STATISTICS {stats_name}"
        sql += f"\nON {full_name} ({column})"
        
        # Build WITH clause
        if operation.with_fullscan:
            sql += "\nWITH FULLSCAN"
        elif operation.sample_percent:
            sql += f"\nWITH SAMPLE {operation.sample_percent} PERCENT"
        
        return sql
    
    def _build_update_statistics(self, operation: BaseOperation) -> str:  # UpdateStatistics not yet defined
        """Build UPDATE STATISTICS statement.
        
        Note: Fabric automatically updates statistics for Delta tables.
        """
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        
        sql = f"UPDATE STATISTICS {full_name}"
        
        if operation.columns:
            columns = ", ".join([self.quote_identifier(col) for col in operation.columns])
            sql += f" ({columns})"
        
        if operation.sample_percent:
            sql += f" WITH SAMPLE {operation.sample_percent} PERCENT"
        elif operation.fullscan:
            sql += " WITH FULLSCAN"
        
        return sql
    
    def _build_create_index(self, operation: BaseOperation) -> str:  # CreateIndex not yet defined
        """Build CREATE INDEX statement.
        
        Note: Fabric Delta tables use file-level statistics and Z-ordering
        instead of traditional indexes.
        """
        # For Fabric, we might want to use OPTIMIZE with Z-ORDER instead
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        
        if operation.index_type == "ZORDER":
            # Use OPTIMIZE with Z-ORDER
            columns = ", ".join([self.quote_identifier(col) for col in operation.columns])
            return f"OPTIMIZE {full_name} ZORDER BY ({columns})"
        else:
            # Traditional index (may not be supported)
            index_name = self.quote_identifier(operation.index_name)
            columns = ", ".join([self.quote_identifier(col) for col in operation.columns])
            
            sql = f"CREATE"
            
            if operation.unique:
                sql += " UNIQUE"
            
            if operation.index_type:
                sql += f" {operation.index_type}"
            
            sql += f" INDEX {index_name}"
            sql += f"\nON {full_name} ({columns})"
            
            return sql
    
    def _build_create_schema(self, operation: CreateSchema) -> str:
        """Build CREATE SCHEMA statement."""
        schema_name = self.quote_identifier(operation.schema)
        
        sql = f"CREATE SCHEMA"
        
        if operation.if_not_exists:
            sql += " IF NOT EXISTS"
        
        sql += f" {schema_name}"
        
        if operation.authorization:
            sql += f" AUTHORIZATION {operation.authorization}"
        
        return sql
    
    def _build_drop_schema(self, operation: DropSchema) -> str:
        """Build DROP SCHEMA statement."""
        schema_name = self.quote_identifier(operation.schema)
        
        if operation.cascade:
            cascade_clause = " CASCADE"
        elif operation.restrict:
            cascade_clause = " RESTRICT"
        else:
            cascade_clause = ""
        
        if operation.if_exists:
            # Fabric supports DROP SCHEMA IF EXISTS
            return f"DROP SCHEMA IF EXISTS {schema_name}{cascade_clause}"
        else:
            return f"DROP SCHEMA {schema_name}{cascade_clause}"
    
    def _build_select(self, operation: Select) -> str:
        """Build SELECT statement."""
        full_name = self.fully_qualified_name(operation.schema, operation.object_name)
        
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
        
        # Add LIMIT/OFFSET (Fabric uses TOP and OFFSET...FETCH)
        if operation.limit is not None:
            if operation.offset is not None:
                # Use OFFSET...FETCH for pagination
                sql += f" OFFSET {operation.offset} ROWS FETCH NEXT {operation.limit} ROWS ONLY"
            else:
                # Use TOP for simple limiting (more efficient)
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
            # OFFSET without LIMIT
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
            r'sp_execute_external_script',
            r'OPENROWSET.*BULK',  # Prevent bulk admin operations
            r'OPENDATASOURCE'
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
    
    # ========== Helper Methods ==========
    
    def format_column_definitions(self, columns: List[ColumnDefinition]) -> str:
        """Format column definitions for CREATE TABLE.
        
        Microsoft Fabric Warehouse constraint support:
        - PRIMARY KEY: Supported with NONCLUSTERED NOT ENFORCED only
        - UNIQUE: Supported with NOT ENFORCED only
        - CHECK: Not supported (ignored)
        - NOT NULL: Supported and enforced
        - DEFAULT: Supported
        
        Constraints are used for query optimization but not enforced.
        Users must ensure data integrity through ETL/ELT processes.
        
        Args:
            columns: List of column definitions
            
        Returns:
            Column definitions for CREATE TABLE with Fabric-specific constraints
        """
        column_defs = []
        for col in columns:
            col_def = f"{self.quote_identifier(col.name)} {col.data_type}"
            
            if col.nullable is False:
                col_def += " NOT NULL"
            
            if col.default_value is not None:
                col_def += f" DEFAULT {col.default_value}"
            
            # Handle PRIMARY KEY - must be NONCLUSTERED and NOT ENFORCED
            if col.primary_key:
                col_def += " PRIMARY KEY NONCLUSTERED NOT ENFORCED"
            # Handle UNIQUE - must be NOT ENFORCED
            elif col.unique:
                col_def += " UNIQUE NOT ENFORCED"
            
            # CHECK constraints are not supported in Fabric Warehouse - ignore
            # if col.check_constraint: 
            #     # Not supported - will be ignored
            
            column_defs.append(col_def)
        
        return ",\n    ".join(column_defs)
    
    
    def optimize_table(self, schema: str, table_name: str, z_order_by: Optional[List[str]] = None) -> str:
        """Generate OPTIMIZE statement for Delta table optimization.
        
        This is a Fabric-specific feature for optimizing Delta tables.
        """
        full_name = self.fully_qualified_name(schema, table_name)
        
        sql = f"OPTIMIZE {full_name}"
        
        if z_order_by:
            columns = ", ".join([self.quote_identifier(col) for col in z_order_by])
            sql += f" ZORDER BY ({columns})"
        
        return sql
    
    def vacuum_table(self, schema: str, table_name: str, retention_hours: int = 168) -> str:
        """Generate VACUUM statement for Delta table maintenance.
        
        Removes old files that are no longer referenced by the Delta table.
        Default retention is 7 days (168 hours).
        """
        full_name = self.fully_qualified_name(schema, table_name)
        return f"VACUUM {full_name} RETAIN {retention_hours} HOURS"