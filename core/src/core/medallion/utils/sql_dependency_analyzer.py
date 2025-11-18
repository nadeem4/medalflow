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

import re
from typing import Dict, List, Set, Optional, Any, TYPE_CHECKING
from enum import Enum
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from core.logging import get_logger
from core.observability.context import sanitize_extras
from core.types.metadata import SQLDependencies
from core.operations import BaseOperation
from core.constants.sql import QueryType

if TYPE_CHECKING:
    from core.settings import _Settings

logger = get_logger(__name__)



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
    
    def __init__(self, settings: "_Settings"):
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
        
        parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        
        # Extract CTEs first (to exclude from source tables)
        ctes = self._extract_ctes_sqlglot(parsed)
        
        # Extract source tables (excluding CTEs)
        reads_from = self._extract_source_tables_sqlglot(parsed, ctes)
        
        # Extract target table directly from SQL
        writes_to = self._extract_target_table_sqlglot(parsed)
        
        return SQLDependencies(
            reads_from=reads_from,
            writes_to=writes_to
        )
    
    def _extract_source_tables_sqlglot(self, ast: 'exp.Expression', ctes: Set[str]) -> Dict[str, Set]:
        """Extract all source tables from SQLGlot AST.
        
        Args:
            ast: SQLGlot expression tree
            cte_names: Set of CTE names to exclude
            
        Returns:
            Set of fully qualified table names
        """
        tables: Dict[str, Set] = {}

        # Find all table references
        for table in ast.find_all(exp.Table):
            if table:
                table_parts = self._table_parts(table)
                full_table_name = f"{ '.'.join([val for val in table_parts.values() if val])}"
                if self._is_cte(full_table_name, ctes):
                    continue

                if table_parts['schema'] not in tables:
                    tables[table_parts['schema']] = set()
                tables[table_parts['schema']].add(table_parts['table'])
        return tables

    
    def _extract_target_table_sqlglot(self, ast: 'exp.Expression') -> Optional[str]:
        """Extract target table for DML operations from SQLGlot AST.
        
        Args:
            ast: SQLGlot expression tree
            
        Returns:
            Fully qualified target table name or None
        """
        if isinstance(ast.this, exp.Table):
            table_parts = self._table_parts(ast.this)
            return f"{ '.'.join([val for val in table_parts.values() if val])}"
        return None
    
    def _extract_ctes_sqlglot(self, ast: 'exp.Expression') -> Set[str]:
        """Extract CTE names from SQLGlot AST.
        
        Args:
            ast: SQLGlot expression tree
            
        Returns:
            List of CTE names defined in the query
        """
        return Set([cte.alias for cte in ast.find_all(exp.CTE) if hasattr(cte, 'alias') and cte.alias])
        

    
    
    def _is_cte(self, table_name: str, cte_names: Set[str]) -> bool:
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
        table_parts = table_name.split('.')
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
        return {'database': table.catalog, 'schema': table.db, 'table': table.name}
    
    def analyze_operations(self, operations: List[BaseOperation]) -> Dict[BaseOperation, SQLDependencies]:
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
        from core.query_builder.factory import QueryBuilderFactory
        
        operation_dependencies = {}
        query_builder = QueryBuilderFactory.create()
        
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
                            "schema": getattr(operation, "schema_name", None) or getattr(operation, "schema", None),
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
                    writes_to=f"{query_builder.fully_qualified_name(schema=operation.schema, object_name=operation.object_name)}" if operation.operation_type in [
                        QueryType.CREATE_TABLE,
                        QueryType.INSERT,
                        QueryType.UPDATE,
                        QueryType.MERGE,
                        QueryType.DELETE
                    ] else None
                )
        
        return operation_dependencies
    
