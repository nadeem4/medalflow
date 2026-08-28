"""MedalFlow compute module for platform-agnostic data processing.

This module provides abstractions for working with a compute platform
(Synapse) and its SQL engine in a unified, operation-based way.

Overview:
    The compute module implements a clean operation-based architecture where
    all database operations are represented as data classes. Platforms handle
    the execution details internally, maintaining complete platform independence
    for the medallion layer.

Architecture:
    The module is organized into several layers:

    1. **Operations Layer**: Data classes representing database operations
       - CreateTable, Insert, Update, Delete, Merge, etc.
       - Platform-agnostic operation definitions (see medalflow.operations)

    2. **Platform Layer**: Manages platform-specific execution
       - _BasePlatform: Abstract base defining the platform interface
       - SynapsePlatform: Azure Synapse implementation

    3. **Engine Layer**: Handles actual query execution
       - BaseSQLEngine: Abstract base for SQL engines
       - SynapseSQLEngine: SQL query execution against the configured pool

    4. **Query Builder Layer**: Generates platform-specific SQL
       - BaseQueryBuilder: Abstract interface for query builders
       - SynapseServerlessQueryBuilder: T-SQL generation

    5. **Factory Layer**: Creates platform instances
       - create_platform(): builds the platform named by
         ``settings.compute.compute_type``

Key Features:
    - **Operation-Based**: All operations are data, platforms handle execution
    - **Platform Abstraction**: The medallion layer never sees platform details
    - **Type Safety**: Full type hints and pydantic validation

Configuration:
    The compute module is configured through ``medalflow.settings.ComputeSettings``,
    which reads its values from the environment. See that class for the full
    set of options, including the platform selector ``compute_type``.

Example Usage:

    >>> from medalflow.compute import create_platform, CreateTable, Insert
    >>> from medalflow.compute import CreateStatistics
    >>>
    >>> # Get platform (configured via settings)
    >>> platform = create_platform()
    >>>
    >>> # Create table
    >>> create_op = CreateTable(
    ...     schema_name="silver",
    ...     object_name="customers",
    ...     select_query="SELECT * FROM bronze.raw_customers WHERE active = 1"
    ... )
    >>> result = platform.execute_operation(create_op)
    >>> print(f"Table created: {result.success}")
    >>>
    >>> # Insert data
    >>> insert_op = Insert(
    ...     schema_name="silver",
    ...     object_name="customers",
    ...     source_query="SELECT * FROM staging.new_customers"
    ... )
    >>> result = platform.execute_operation(insert_op)
    >>> print(f"Rows inserted: {result.rows_affected}")
    >>>
    >>> # Create statistics (Synapse allows one column per statistic)
    >>> stats_op = CreateStatistics(
    ...     schema_name="silver",
    ...     object_name="customers",
    ...     columns=["customer_id"]
    ... )
    >>> result = platform.execute_operation(stats_op)

See Also:
    - medalflow.settings.ComputeSettings: Configuration options
    - medalflow.compute.platforms: Platform implementations
    - medalflow.compute.engines: Engine implementations
    - medalflow.compute.types: Operation result types
"""

# Import operations from new location
from medalflow.compute.factory import create_platform
from medalflow.compute.platforms.synapse import SynapsePlatform

# Import compute-specific types (results and configs)
from medalflow.compute.types import OperationResult
from medalflow.constants.compute import ComputeEnvironment, EngineType, ResultFormat
from medalflow.operations import (
    BaseOperation,
    Copy,
    CreateOrAlterView,
    CreateSchema,
    CreateStatistics,
    CreateTable,
    Delete,
    DropTable,
    DropView,
    ExecuteSQL,
    Insert,
    Merge,
    QueryContext,
    Update,
)

# Import protocol types
from medalflow.operations.columns import ColumnDefinition

__all__ = [
    # Operations (public)
    "BaseOperation",
    "CreateTable",
    "DropTable",
    "Insert",
    "Update",
    "Delete",
    "Merge",
    "Copy",
    "CreateOrAlterView",
    "DropView",
    "CreateStatistics",
    "CreateSchema",
    "ExecuteSQL",
    # Operation metadata (public)
    "ColumnDefinition",
    "QueryContext",
    # Results (public)
    "OperationResult",
    # Constants (public)
    "ComputeEnvironment",
    "EngineType",
    "ResultFormat",
    # Factory
    "create_platform",
    "SynapsePlatform",
]
