"""MedalFlow compute module for platform-agnostic data processing.

This module provides abstractions for working with different compute platforms
(Synapse, Fabric) and engines (SQL, Spark) in a unified, operation-based way.

Overview:
    The compute module implements a clean operation-based architecture where
    all database operations are represented as data classes. Platforms handle
    the execution details internally, maintaining complete platform independence
    for the medallion layer.

Architecture:
    The module is organized into several layers:
    
    1. **Operations Layer**: Data classes representing database operations
       - CreateTable, Insert, Update, Delete, Merge, etc.
       - Platform-agnostic operation definitions
    
    2. **Platform Layer**: Manages platform-specific execution
       - BasePlatform: Protocol defining platform interface
       - SynapsePlatform: Azure Synapse implementation
       - FabricPlatform: Microsoft Fabric implementation
    
    3. **Engine Layer**: Handles actual query execution
       - BaseEngine: Abstract base for all engines
       - SQLEngine: SQL query execution (platform-specific)
       - SparkEngine: Spark job submission and monitoring
    
    4. **Query Builder Layer**: Generates platform-specific SQL
       - BaseQueryBuilder: Abstract interface for query builders
       - Platform-specific builders for SQL generation
    
    5. **Factory Layer**: Creates platform instances
       - PlatformFactory: Manages platform registration and creation

Key Features:
    - **Operation-Based**: All operations are data, platforms handle execution
    - **Platform Abstraction**: Write once, run on multiple platforms
    - **Engine Selection**: Automatic SQL vs Spark selection per operation
    - **Batch Operations**: Execute multiple operations efficiently
    - **Transaction Support**: Where platform supports it
    - **Type Safety**: Full type hints and validation
    - **Extensibility**: Easy to add new platforms via factory pattern

Configuration:
    The compute module is configured through environment variables with the
    CTE_COMPUTE__ prefix. Key settings include:
    
    - CTE_COMPUTE__COMPUTE_TYPE: Platform type (synapse/fabric)
    - CTE_COMPUTE__SYNAPSE__CONNECTION__ENDPOINT: Synapse SQL endpoint
    - CTE_COMPUTE__FABRIC__WORKSPACE__ID: Fabric workspace identifier
    
    See core.settings.ComputeSettings for full configuration options.

Example Usage:
    
    >>> from core.compute import PlatformFactory, CreateTable, Insert
    >>> from core.compute import CreateStatistics
    >>> 
    >>> # Get platform (configured via settings)
    >>> platform = PlatformFactory.create_platform()
    >>> 
    >>> # Create table
    >>> create_op = CreateTable(
    ...     schema="silver",
    ...     object_name="customers",
    ...     select_query="SELECT * FROM bronze.raw_customers WHERE active = 1"
    ... )
    >>> result = platform.execute(create_op)
    >>> print(f"Table created: {result.success}")
    >>> 
    >>> # Insert data
    >>> insert_op = Insert(
    ...     schema="silver",
    ...     object_name="customers",
    ...     source_query="SELECT * FROM staging.new_customers"
    ... )
    >>> result = platform.execute(insert_op)
    >>> print(f"Rows inserted: {result.rows_affected}")
    >>> 
    >>> # Create statistics
    >>> stats_op = CreateStatistics(
    ...     schema="silver",
    ...     object_name="customers",
    ...     columns=["customer_id", "region"]
    ... )
    >>> result = platform.execute(stats_op)
    >>> 
    >>> # Batch operations
    >>> operations = [create_op, insert_op, stats_op]
    >>> batch_result = platform.execute_batch(operations, transaction=True)
    >>> print(f"Success rate: {batch_result.success_rate}%")

See Also:
    - core.settings.ComputeSettings: Configuration options
    - core.compute.platforms: Platform implementations
    - core.compute.engines: Engine implementations
    - core.compute.types: Operation definitions
"""

# Import operations from new location
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
    ExecuteSQL,
    QueryContext,
)

# Import protocol types
from core.protocols.operations import ColumnDefinition

# Import compute-specific types (results and configs)
from core.compute.types import (
    OperationResult,
    BatchOperationResult,
    SparkJobConfig,
    JobResult,
)

from core.constants.compute import ComputeEnvironment, EngineType, JobStatus, ResultFormat


from core.compute.factory import get_platform_factory
from core.compute.platforms.synapse import SynapsePlatform
from core.compute.platforms.fabric import FabricPlatform



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
    "BatchOperationResult",
    
    # Constants (public)
    "ComputeEnvironment",
    "EngineType",
    "JobStatus",
    "ResultFormat",
    
    

    "SparkJobConfig",
    "JobResult",

    # Factory
    "get_platform_factory",
    "SynapsePlatform",
    "FabricPlatform",
]