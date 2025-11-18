import re
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, TYPE_CHECKING

# Import operation types from Layer 1
from core.operations import (
    BaseOperation,
    CreateTable,
    DropTable,
    Insert,
    Update,
    Delete,
    Merge,
    Copy,
    CreateOrAlterView,
    DropView,
    CreateStatistics,
    CreateSchema,
    DropSchema,
    Select,
    ExecuteSQL
)

from core.protocols.operations import ColumnDefinition
from core.constants.sql import QueryType
from core.settings import _Settings



class BaseQueryBuilder(ABC):
    """Base interface for query builders with SQL injection protection.
    
    This abstract base class defines the contract for all platform-specific query
    builders in the medalflow framework. It provides a standardized interface for
    generating SQL statements while ensuring security through input validation.
    
    Query builders are responsible for generating SQL statements for their respective
    platforms. They do NOT execute queries - that responsibility belongs to the
    engine classes.
    
    This class is in Layer 1 (Infrastructure) as it provides fundamental SQL
    generation capabilities that can be used by multiple Layer 2 modules.
    
    Security Principles:
        1. **Input Validation**: All user-provided identifiers are validated before use
        2. **No Direct Concatenation**: Never concatenate unvalidated input into SQL
        3. **Whitelist Approach**: Only allow known-safe characters in identifiers
        4. **Length Limits**: Enforce maximum lengths to prevent buffer overflow attacks
        5. **Platform Awareness**: Respect platform-specific SQL syntax and limitations
    """
    
    def __init__(self, settings: _Settings):
        """Initialize query builder with optional table prefix.
        
        Args:
            table_prefix: Optional prefix to add to table names (e.g., 'sap_', 'oracle_').
                         If not provided, no prefix will be added to table names.
        """
        
        self.settings = settings
        self.table_prefix = settings.table_prefix
        self.skip_prefix_on_schema = settings.compute.active_config.skip_prefix_on_schema
    
    
    @abstractmethod
    def _build_create_table(self, operation: CreateTable) -> str:
        """Build CREATE TABLE statement.
        
        Args:
            operation: CreateTable operation
            
        Returns:
            Platform-specific CREATE TABLE statement
        """
        pass
    
    @abstractmethod
    def _build_drop_table(self, operation: DropTable) -> str:
        """Build DROP TABLE statement.
        
        Args:
            operation: DropTable operation
            
        Returns:
            Platform-specific DROP TABLE statement
        """
        pass
    
    @abstractmethod
    def _build_insert(self, operation: Insert) -> str:
        """Build INSERT statement.
        
        Args:
            operation: Insert operation
            
        Returns:
            Platform-specific INSERT statement
        """
        pass
    
    @abstractmethod
    def _build_update(self, operation: Update) -> str:
        """Build UPDATE statement.
        
        Args:
            operation: Update operation
            
        Returns:
            Platform-specific UPDATE statement
        """
        pass
    
    @abstractmethod
    def _build_delete(self, operation: Delete) -> str:
        """Build DELETE statement.
        
        Args:
            operation: Delete operation
            
        Returns:
            Platform-specific DELETE statement
        """
        pass
    
    @abstractmethod
    def _build_merge(self, operation: Merge) -> str:
        """Build MERGE statement.
        
        Args:
            operation: Merge operation
            
        Returns:
            Platform-specific MERGE statement
        """
        pass
    
    @abstractmethod
    def _build_copy(self, operation: Copy) -> str:
        """Build COPY statement.
        
        Args:
            operation: Copy operation
            
        Returns:
            Platform-specific COPY statement
        """
        pass
    
    @abstractmethod
    def _build_create_or_alter_view(self, operation: CreateOrAlterView) -> str:
        """Build CREATE OR ALTER VIEW statement.
        
        Args:
            operation: CreateOrAlterView operation
            
        Returns:
            Platform-specific CREATE OR ALTER VIEW statement
        """
        pass
    
    @abstractmethod
    def _build_drop_view(self, operation: DropView) -> str:
        """Build DROP VIEW statement.
        
        Args:
            operation: DropView operation
            
        Returns:
            Platform-specific DROP VIEW statement
        """
        pass
    
    def _validate_create_statistics(self, operation: CreateStatistics) -> None:
        """Validate CREATE STATISTICS operation.
        
        Both Synapse Serverless and Fabric Warehouse only support single-column statistics.
        This method enforces that constraint at the query builder level.
        
        Args:
            operation: CreateStatistics operation to validate
            
        Raises:
            ValueError: If validation fails
        """
        if not operation.columns:
            raise ValueError(
                f"Cannot create statistics on {operation.full_object_name}: "
                "No columns specified. Statistics operations require exactly one column."
            )
        
        if len(operation.columns) > 1:
            raise ValueError(
                f"Cannot create statistics on {operation.full_object_name}: "
                f"Multiple columns specified ({', '.join(operation.columns)}). "
                "Both Synapse and Fabric only support single-column statistics. "
                "Create separate statistics for each column."
            )
    
    @abstractmethod
    def _build_create_statistics(self, operation: CreateStatistics) -> str:
        """Build CREATE STATISTICS statement.
        
        Args:
            operation: CreateStatistics operation (guaranteed to have exactly one column)
            
        Returns:
            Platform-specific CREATE STATISTICS statement
        """
        pass
    
    @abstractmethod
    def _build_create_schema(self, operation: CreateSchema) -> str:
        """Build CREATE SCHEMA statement.
        
        Args:
            operation: CreateSchema operation
            
        Returns:
            Platform-specific CREATE SCHEMA statement
        """
        pass
    
    @abstractmethod
    def _build_drop_schema(self, operation: DropSchema) -> str:
        """Build DROP SCHEMA statement.
        
        Args:
            operation: DropSchema operation
            
        Returns:
            Platform-specific DROP SCHEMA statement
        """
        pass
    
    @abstractmethod
    def _build_select(self, operation: Select) -> str:
        """Build SELECT statement.
        
        Args:
            operation: Select operation
            
        Returns:
            Platform-specific SELECT statement
        """
        pass
    
    @abstractmethod
    def _build_execute_sql(self, operation: ExecuteSQL) -> str:
        """Build/validate arbitrary SQL statement.
        
        Args:
            operation: ExecuteSQL operation
            
        Returns:
            Validated SQL statement
        """
        pass
    
    def build_query(self, operation: BaseOperation) -> str:
        """Build SQL query from operation.
        
        Main method that converts operations into platform-specific SQL queries.
        
        Args:
            operation: Operation to convert to SQL
            
        Returns:
            Platform-specific SQL query
            
        Raises:
            NotImplementedError: If operation type is not supported
            ValueError: If operation validation fails
        """
        
        # Special validation for CREATE_STATISTICS
        if operation.operation_type == QueryType.CREATE_STATISTICS:
            self._validate_create_statistics(operation)
        
        # Map operation type to builder method
        operation_mapping = {
            QueryType.CREATE_TABLE: self._build_create_table,
            QueryType.DROP_TABLE: self._build_drop_table,
            QueryType.INSERT: self._build_insert,
            QueryType.UPDATE: self._build_update,
            QueryType.DELETE: self._build_delete,
            QueryType.MERGE: self._build_merge,
            QueryType.COPY: self._build_copy,
            QueryType.CREATE_OR_ALTER_VIEW: self._build_create_or_alter_view,
            QueryType.DROP_VIEW: self._build_drop_view,
            QueryType.CREATE_STATISTICS: self._build_create_statistics,
            QueryType.CREATE_SCHEMA: self._build_create_schema,
            QueryType.DROP_SCHEMA: self._build_drop_schema,
            QueryType.SELECT: self._build_select,
            QueryType.EXECUTE_SQL: self._build_execute_sql,
        }
        
        builder_method = operation_mapping.get(operation.operation_type)
        if builder_method:
            return builder_method(operation)
        
        raise NotImplementedError(
            f"Operation type {operation.operation_type} not supported by {self.__class__.__name__}"
        )
        
    def fully_qualified_name(self, schema: str, object_name: str) -> str:
        """Build fully qualified object name with appropriate prefix.
        
        Applies data source prefix based on configuration settings.
        Schemas listed in skip_prefix_on_schema setting will not have
        prefixes added to their table names.
        
        Args:
            schema: Schema name
            object_name: Object name (table/view)
            
        Returns:
            Fully qualified name like [schema].[prefixed_object_name]
            with all identifiers properly quoted
        """
        quoted_schema = self.quote_identifier(schema, "schema")
       
        
        if schema.lower() in self.skip_prefix_on_schema:
            quoted_object = self.quote_identifier(object_name, "object")
            return f"{quoted_schema}.{quoted_object}"
        
        quoted_object = self.quote_identifier(f"{self.table_prefix}{object_name}", "object")
        return f"{quoted_schema}.{quoted_object}"
    
    def quote_identifier(self, identifier: str, identifier_type: str = "identifier") -> str:
        """Quote an identifier for safe SQL usage.
        
        Default implementation uses square brackets.
        Override for platform-specific quoting (e.g., double quotes).
        
        Args:
            identifier: Identifier to quote
            
        Returns:
            Quoted identifier
        """
        identifier = identifier.strip().replace("]", '').replace("[", '')
        self._validate_identifier(identifier, identifier_type)
        return f"[{identifier}]"
    
    def quote_string(self, value: str) -> str:
        """Quote a string value for SQL.
        
        Args:
            value: String value to quote
            
        Returns:
            Properly quoted and escaped string
        """
        # Escape single quotes by doubling them
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    
    def format_column_list(self, columns: List[str]) -> str:
        """Format a list of columns for SQL.
        
        Args:
            columns: List of column names
            
        Returns:
            Comma-separated list of quoted columns
        """
        return ", ".join(self.quote_identifier(col) for col in columns)
    
    def format_value_list(self, values: List[Any]) -> str:
        """Format a list of values for SQL.
        
        Args:
            values: List of values
            
        Returns:
            Comma-separated list of properly formatted values
        """
        formatted = []
        for value in values:
            if value is None:
                formatted.append("NULL")
            elif isinstance(value, str):
                formatted.append(self.quote_string(value))
            elif isinstance(value, bool):
                formatted.append("1" if value else "0")
            else:
                formatted.append(str(value))
        return ", ".join(formatted)
    
    def format_set_clause(self, columns: Dict[str, Any]) -> str:
        """Format SET clause for UPDATE.
        
        Args:
            columns: Dictionary of column -> value/expression
            
        Returns:
            SET clause like "col1 = val1, col2 = val2"
        """
        assignments = []
        for col, value in columns.items():
            col_quoted = self.quote_identifier(col)
            if value is None:
                assignments.append(f"{col_quoted} = NULL")
            elif isinstance(value, str):
                # Check if it's an expression (contains SQL keywords/functions)
                if self._is_expression(value):
                    assignments.append(f"{col_quoted} = {value}")
                else:
                    assignments.append(f"{col_quoted} = {self.quote_string(value)}")
            elif isinstance(value, bool):
                assignments.append(f"{col_quoted} = {1 if value else 0}")
            else:
                assignments.append(f"{col_quoted} = {value}")
        return ", ".join(assignments)
    
    def format_column_definitions(self, columns: List[ColumnDefinition]) -> str:
        """Format column definitions for CREATE TABLE.
        
        Args:
            columns: List of column definitions
            
        Returns:
            Column definitions for CREATE TABLE
        """
        definitions = []
        for col in columns:
            definition = f"{self.quote_identifier(col.name)} {col.data_type}"
            
            if not col.nullable:
                definition += " NOT NULL"
            
            if col.default_value is not None:
                if isinstance(col.default_value, str):
                    definition += f" DEFAULT {self.quote_string(col.default_value)}"
                else:
                    definition += f" DEFAULT {col.default_value}"
            
            if col.primary_key:
                definition += " PRIMARY KEY"
            elif col.unique:
                definition += " UNIQUE"
            
            if col.check_constraint:
                definition += f" CHECK ({col.check_constraint})"
            
            definitions.append(definition)
        
        return ", ".join(definitions)
    
    def _validate_identifier(self, identifier: str, identifier_type: str = "identifier") -> None:
        """Validate an identifier for SQL injection protection.
        
        Args:
            identifier: The identifier to validate
            identifier_type: Type of identifier for error messages
            
        Raises:
            ValueError: If identifier is invalid
        """
        if not identifier:
            raise ValueError(f"Empty {identifier_type} name")
        
        # Check length (max 128 characters for most databases)
        if len(identifier) > 128:
            raise ValueError(f"{identifier_type} name too long: {identifier}")
        
        # Check for valid characters (alphanumeric, underscore, dash)
        # This regex allows letters, numbers, underscores, and hyphens
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_\-]*$', identifier):
            raise ValueError(f"Invalid {identifier_type} name: {identifier}")
        
        # Check for SQL injection patterns
        dangerous_patterns = [
            r';\s*DROP',
            r';\s*DELETE',
            r';\s*UPDATE',
            r';\s*INSERT',
            r'--',
            r'/\*',
            r'\*/',
            r'UNION\s+SELECT',
            r'OR\s+1\s*=\s*1',
            r"OR\s+'1'\s*=\s*'1'"
        ]
        
        identifier_upper = identifier.upper()
        for pattern in dangerous_patterns:
            if re.search(pattern, identifier_upper):
                raise ValueError(f"Potentially dangerous {identifier_type} name: {identifier}")
    
    def _is_expression(self, value: str) -> bool:
        """Check if a string value is a SQL expression.
        
        Args:
            value: String value to check
            
        Returns:
            True if value appears to be a SQL expression
        """
        # Common SQL functions and keywords that indicate an expression
        expression_patterns = [
            r'\bGETDATE\b',
            r'\bNOW\b',
            r'\bCURRENT_TIMESTAMP\b',
            r'\bCAST\b',
            r'\bCONVERT\b',
            r'\bCASE\b',
            r'\bWHEN\b',
            r'\bCOALESCE\b',
            r'\bISNULL\b',
            r'\bNULLIF\b',
            r'\+',  # Arithmetic operators
            r'\-',
            r'\*',
            r'/',
            r'\(',  # Function calls
        ]
        
        value_upper = value.upper()
        for pattern in expression_patterns:
            if re.search(pattern, value_upper, re.IGNORECASE):
                return True
        return False
    
    def build_select_all(self, schema: str, object_name: str) -> str:
        """Build SELECT * query with proper table naming.
        
        Args:
            schema: Schema name
            object_name: Table/view name (without prefix)
            
        Returns:
            SELECT * query with fully qualified table name
        """
        full_name = self.fully_qualified_name(schema, object_name)
        return f"SELECT * FROM {full_name}"
    
    def build_select_columns(self, schema: str, object_name: str, columns: List[str]) -> str:
        """Build SELECT query with specific columns.
        
        Args:
            schema: Schema name
            object_name: Table/view name (without prefix)
            columns: List of column names to select
            
        Returns:
            SELECT query with specified columns and fully qualified table name
        """
        if not columns:
            return self.build_select_all(schema, object_name)
        
        full_name = self.fully_qualified_name(schema, object_name)
        column_list = self.format_column_list(columns)
        return f"SELECT {column_list} FROM {full_name}"
    
    def build_select_where(self, schema: str, object_name: str, where_clause: str, 
                          columns: Optional[List[str]] = None) -> str:
        """Build SELECT query with WHERE clause.
        
        Args:
            schema: Schema name
            object_name: Table/view name (without prefix)
            where_clause: WHERE condition (without the WHERE keyword)
            columns: Optional list of columns (defaults to *)
            
        Returns:
            SELECT query with WHERE clause
        """
        full_name = self.fully_qualified_name(schema, object_name)
        
        if columns:
            column_list = self.format_column_list(columns)
        else:
            column_list = "*"
        
        return f"SELECT {column_list} FROM {full_name} WHERE {where_clause}"
    
    def build_select_where_not(self, schema: str, object_name: str, where_clause: str,
                               columns: Optional[List[str]] = None) -> str:
        """Build SELECT query with WHERE NOT condition.
        
        Useful for DELETE operations where we need to select rows to keep
        (those that don't match the delete condition).
        
        Args:
            schema: Schema name
            object_name: Table/view name (without prefix)
            where_clause: Condition to negate (without the WHERE keyword)
            columns: Optional list of columns (defaults to *)
            
        Returns:
            SELECT query with WHERE NOT (condition)
        """
        full_name = self.fully_qualified_name(schema, object_name)
        
        if columns:
            column_list = self.format_column_list(columns)
        else:
            column_list = "*"
        
        return f"SELECT {column_list} FROM {full_name} WHERE NOT ({where_clause})"
    
    def _validate_sql_expression(self, expression: str, expression_type: str = "expression") -> None:
        """Validate a SQL expression for injection.
        
        Less strict than identifier validation since expressions can contain
        SQL keywords, but still checks for dangerous patterns.
        
        Args:
            expression: SQL expression to validate
            expression_type: Type for error messages
            
        Raises:
            ValueError: If expression contains dangerous patterns
        """
        if not expression:
            return
        
        # Check for dangerous patterns that shouldn't be in expressions
        dangerous_patterns = [
            r';\s*DROP\s+TABLE',
            r';\s*DROP\s+DATABASE',
            r';\s*DELETE\s+FROM',
            r';\s*TRUNCATE',
            r'EXEC\s*\(',
            r'EXECUTE\s+IMMEDIATE',
            r'xp_cmdshell'
        ]
        
        expression_upper = expression.upper()
        for pattern in dangerous_patterns:
            if re.search(pattern, expression_upper):
                raise ValueError(f"Potentially dangerous {expression_type}: {expression}")