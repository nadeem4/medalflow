"""Compute-related constants and enumerations.

This module defines type-safe enumerations used throughout the compute
module for consistent behavior across platforms and engines.
"""

from enum import Enum


class ComputeType(str, Enum):
    """Type of compute platform.
    
    Defines the available compute platform types that can be used
    for data processing and query execution.
    
    Values:
        SYNAPSE: Azure Synapse Analytics
            - Dedicated SQL pools
            - Serverless SQL pools  
            - Apache Spark pools
            - External table support
            
        FABRIC: Microsoft Fabric
            - Lakehouse SQL endpoint
            - SQL Data Warehouse
            - Spark compute
            - Direct lake access
    """
    
    SYNAPSE = "synapse"
    FABRIC = "fabric"


class ComputeEnvironment(str, Enum):
    """Compute environment configuration.
    
    Defines the operational context for compute operations, allowing
    platforms to optimize resource allocation and configuration based
    on workload characteristics.
    
    Values:
        ETL: Extract-Transform-Load workload environment.
            - Optimized for: Batch processing, high throughput
            - Characteristics: Large resource pools, longer timeouts
            - Use for: Data pipelines, scheduled jobs, bulk operations
        
        CONSUMPTION: Interactive/analytical workload environment.
            - Optimized for: Query performance, low latency
            - Characteristics: Smaller pools, shorter timeouts, caching
            - Use for: Dashboards, ad-hoc queries, reporting
    
    Example:
        >>> # For batch processing
        >>> platform = factory.create_platform(environment=ComputeEnvironment.ETL)
        >>> 
        >>> # For interactive queries
        >>> platform = factory.create_platform(environment=ComputeEnvironment.CONSUMPTION)
    """
    
    ETL = "etl"
    CONSUMPTION = "consumption"


class EngineType(str, Enum):
    """Available engine types for query execution.
    
    Defines the compute engines available for processing queries and
    transformations. Each platform may support different subsets of engines.
    
    Values:
        SQL: Traditional SQL engine for set-based operations.
            - Best for: Simple queries, small-medium datasets
            - Platforms: All (Synapse SQL, Fabric Warehouse)
        SPARK: Distributed compute engine for large-scale processing.
            - Best for: Complex transformations, large datasets, ML
            - Platforms: Synapse Spark, Fabric Spark
        AUTO: Automatic engine selection based on heuristics.
            - Platform analyzes query characteristics to choose
            - Considers: data volume, complexity, transformations
            - Falls back to platform defaults if unsure
    
    Example:
        >>> # Force SQL engine
        >>> context = QueryContext(preferred_engine=EngineType.SQL)
        >>> 
        >>> # Let platform decide
        >>> context = QueryContext(preferred_engine=EngineType.AUTO)
    """
    
    SQL = "sql"
    SPARK = "spark"
    AUTO = "auto"


class JobStatus(str, Enum):
    """Status of a compute job.
    
    Represents the lifecycle states of a compute job (primarily Spark jobs).
    Used for tracking job progress and determining appropriate actions.
    
    Values:
        PENDING: Job submitted but not yet started.
            - Waiting for resources or in queue
        RUNNING: Job actively executing.
            - Resources allocated, processing data
        SUCCEEDED: Job completed successfully.
            - All tasks finished without errors
        FAILED: Job terminated due to error.
            - Check JobResult.error for details
        CANCELLED: Job terminated by user request.
            - Manual cancellation or system shutdown
        TIMEOUT: Job exceeded time limit.
            - Terminated due to timeout_seconds limit
    
    State Transitions:
        PENDING -> RUNNING -> SUCCEEDED
                         |-> FAILED
                         |-> TIMEOUT
                |-> CANCELLED
    
    Example:
        >>> result = spark_engine.submit_job(query)
        >>> while result.status == JobStatus.RUNNING:
        ...     time.sleep(5)
        ...     result = spark_engine.get_job_status(result.job_id)
        >>> if result.status == JobStatus.SUCCEEDED:
        ...     print("Job completed!")
    """
    
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class ResultFormat(str, Enum):
    """Format for query result data when using ExecuteSQL operation.
    
    Defines how query results should be returned when executing SELECT
    queries or other operations that return data.
    
    Values:
        DATAFRAME: Return results as pandas DataFrame (default).
            - Best for: Data analysis, transformations, visualizations
            - Memory: Loads all data into memory
            - Use when: Working with tabular data in Python
            
        DICT_LIST: Return results as list of dictionaries.
            - Best for: JSON serialization, row-by-row processing
            - Memory: Loads all data into memory as Python objects
            - Use when: Need direct Python dict access
            
        SCALAR: Return single value result.
            - Best for: COUNT, MAX, MIN, single-value queries
            - Memory: Minimal (single value)
            - Use when: Query returns exactly one value
    
    Example:
        >>> # Get results as DataFrame (default)
        >>> op = ExecuteSQL(
        ...     sql="SELECT * FROM customers",
        ...     returns_results=True,
        ...     result_format=ResultFormat.DATAFRAME
        ... )
        >>> 
        >>> # Get results as list of dicts
        >>> op = ExecuteSQL(
        ...     sql="SELECT id, name FROM users",
        ...     returns_results=True,
        ...     result_format=ResultFormat.DICT_LIST
        ... )
        >>> 
        >>> # Get single value
        >>> op = ExecuteSQL(
        ...     sql="SELECT COUNT(*) FROM orders",
        ...     returns_results=True,
        ...     result_format=ResultFormat.SCALAR
        ... )
    """
    
    DATAFRAME = "dataframe"
    DICT_LIST = "dict_list"
    SCALAR = "scalar"