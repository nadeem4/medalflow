"""Base decorators for medallion architecture ETL processes.

This module provides the fundamental decorators used across all medallion layers.
The query_metadata decorator is the primary building block for defining SQL operations
within sequencer classes, providing execution control and dependency management.
"""

from typing import Callable, Optional, Union

from medalflow.constants.compute import EngineType
from medalflow.constants.sql import QueryType
from medalflow.types.metadata import QueryMetadata


def query_metadata(
    type: Union[str, QueryType],
    table_name: str = "",
    schema_name: str = "",
    query: Optional[str] = None,
    name: Optional[str] = None,
    preferred_engine: Union[str, EngineType] = EngineType.SQL,
    unique_idx: Optional[list[str]] = None,
    filter: Optional[str] = None,
    create_stats: bool = False,
    stats_columns: Optional[list[str]] = None,
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
        ...     schema_name="staging"
        ... )
        ... def extract_customers(self) -> str:
        ...     return '''
        ...     SELECT CustomerID, Name, Email, UpdatedDate
        ...     FROM bronze.customers
        ...     WHERE UpdatedDate > ?
        ...     '''

        UPDATE with specific target:
        >>> @query_metadata(
        ...     type=QueryType.UPDATE,
        ...     name="DimCustomer"
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
        engine_type = (
            EngineType(preferred_engine) if isinstance(preferred_engine, str) else preferred_engine
        )

        metadata = QueryMetadata(
            type=query_type,
            table_name=table_name,
            schema_name=schema_name,
            preferred_engine=engine_type,
            unique_idx=unique_idx,
            filter=filter,
            create_stats=create_stats,
            stats_columns=stats_columns,
        )

        func._query_metadata = metadata
        return func

    return decorator
