"""Base decorators for medallion architecture ETL processes.

This module provides the fundamental decorators used across all medallion layers.
The query_metadata decorator is the primary building block for defining SQL operations
within sequencer classes, providing execution control and dependency management.
"""

from typing import Any, Callable, List, Optional, Union

from core.constants.compute import EngineType
from core.constants.medallion import ExecutionMode
from core.constants.sql import QueryType
from core.types.metadata import QueryMetadata


def query_metadata(
    type: Union[str, QueryType],
    table_name: str = "",
    schema_name: str = "",
    execution_type: Union[str, ExecutionMode] = ExecutionMode.SEQUENTIAL,
    order: float = 0.0,
    depends_on: Optional[List[str]] = None,
    query: Optional[str] = None,
    name: Optional[str] = None,
    preferred_engine: Union[str, EngineType] = EngineType.SQL,
    unique_idx: Optional[List[str]] = None,
    filter: Optional[str] = None,
    create_stats: bool = False,
    stats_columns: Optional[List[str]] = None
) -> Callable[[Callable], Callable]:
    """Decorator for query methods within ETL sequencers.
    
    This decorator attaches execution metadata to methods that generate or define
    SQL queries. The framework uses this metadata to determine execution order,
    parallelization opportunities, and dependency management. Each decorated method
    becomes part of the ETL execution plan.
    
    Args:
        type: Type of SQL operation - can be string or QueryType enum.
            Common values: SELECT, INSERT, UPDATE, DELETE, CREATE, MERGE.
            Determines how results are handled and what validations apply.
        table_name: Target table for the operation. For SELECT, this is where
            results are stored. For INSERT/UPDATE/DELETE, this is the affected table.
            Can be empty for operations that don't target a specific table.
        schema_name: Database schema for the target table. If empty, uses the
            default schema from connection settings. Include for cross-schema operations.
        execution_type: [DEPRECATED - IGNORED] Previously controlled parallel vs sequential 
            execution. Now all execution order is determined automatically by analyzing
            data dependencies between queries. Kept for backward compatibility only.
        order: [DEPRECATED - IGNORED] Previously controlled execution priority.
            Now execution order is determined by actual data dependencies in the SQL.
            Kept for backward compatibility only.
        depends_on: [DEPRECATED - IGNORED] Previously created explicit dependencies.
            Now dependencies are automatically detected from SQL analysis.
            Kept for backward compatibility only.
        query: Optional static SQL string. Usually the decorated method returns
            the SQL dynamically, but this allows inline query definition.
            If provided, method return value is ignored.
        name: Required for UPDATE operations to identify the target table.
            Optional identifier for other operations, useful for logging and debugging.
        preferred_engine: Engine preference for query execution. Can be string or
            EngineType enum. Options: SQL (default), SPARK, AUTO. SQL maintains
            backward compatibility. AUTO lets platform analyze query complexity and
            choose optimal engine. Default is SQL.
        unique_idx: List of columns forming the natural/business key for dimensions.
            When specified, indicates this is a dimension table with unique constraints.
        filter: Enum name for filter-based dimensions. When specified and method returns None,
            an enum query is auto-generated from bronze.Enumeration table.
        create_stats: Whether to automatically create statistics after the operation.
            Useful for optimizing query performance on newly created/populated tables.
            Default is False.
        stats_columns: Specific columns to create statistics on. If None and create_stats
            is True, statistics will be created on all columns.
        
    Returns:
        Decorated method with QueryMetadata attached. The framework inspects
        this metadata during execution planning.
        
    Raises:
        ValueError: If UPDATE type is used without name parameter.
        
    Example:
        Basic SELECT with staging:
        >>> @query_metadata(
        ...     type=QueryType.SELECT,
        ...     table_name="CustomerStage",
        ...     schema_name="staging",
        ...     order=1.0
        ... )
        ... def extract_customers(self) -> str:
        ...     return '''
        ...     SELECT CustomerID, Name, Email, UpdatedDate
        ...     FROM bronze.customers
        ...     WHERE UpdatedDate > ?
        ...     '''
        
        Parallel dimension loads:
        >>> @query_metadata(
        ...     type=QueryType.INSERT,
        ...     table_name="DimProduct",
        ...     schema_name="silver",
        ...     execution_type=ExecutionMode.PARALLEL
        ... )
        ... def load_product_dimension(self) -> str:
        ...     return "SELECT * FROM staging.ProductTransform"
        
        Dependent transformations:
        >>> @query_metadata(
        ...     type=QueryType.CREATE_TABLE,
        ...     table_name="CustomerMetrics",
        ...     order=3.0,
        ...     depends_on=["extract_customers", "extract_orders"]
        ... )
        ... def create_customer_metrics(self) -> str:
        ...     return '''
        ...     CREATE TABLE CustomerMetrics AS
        ...     SELECT c.*, COUNT(o.OrderID) as OrderCount
        ...     FROM CustomerStage c
        ...     LEFT JOIN OrderStage o ON c.CustomerID = o.CustomerID
        ...     GROUP BY c.CustomerID
        ...     '''
        
        UPDATE with specific target:
        >>> @query_metadata(
        ...     type=QueryType.UPDATE,
        ...     name="DimCustomer",
        ...     order=5.0
        ... )
        ... def update_customer_status(self) -> str:
        ...     return '''
        ...     UPDATE DimCustomer
        ...     SET Status = 'Inactive', EndDate = GETDATE()
        ...     WHERE CustomerID NOT IN (SELECT CustomerID FROM CustomerStage)
        ...     '''
    
    Notes:
        - Methods can return SQL strings or None (if query parameter is used)
        - The decorator preserves method signature and docstrings
        - Metadata is stored as _query_metadata attribute on the method
        - Framework validates dependency graphs for circular references
        - Use descriptive method names as they appear in logs and dependencies
    """
    def decorator(func: Callable) -> Callable:
        query_type = QueryType(type) if isinstance(type, str) else type
        exec_mode = ExecutionMode(execution_type) if isinstance(execution_type, str) else execution_type
        engine_type = EngineType(preferred_engine) if isinstance(preferred_engine, str) else preferred_engine
        
        
        metadata = QueryMetadata(
            type=query_type,
            table_name=table_name,
            schema_name=schema_name,
            execution_type=exec_mode,
            order=order,
            preferred_engine=engine_type,
            unique_idx=unique_idx,
            filter=filter,
            create_stats=create_stats,
            stats_columns=stats_columns
        )
        
        func._query_metadata = metadata
        return func
    
    return decorator