"""Compute engines for executing queries across different platforms.

This module provides the engine abstraction layer for the MedalFlow compute module.
Engines are responsible for executing SQL queries on their respective platforms.

Overview:
    Engines are the execution layer of the compute module. They handle the actual
    communication with compute platforms, manage connections, execute queries/jobs,
    and handle platform-specific error conditions. Each engine type
    has its own interface and platform-specific implementations.

Architecture:
    The module follows a two-level abstraction pattern:
    
    1. **Base Interfaces**: Abstract classes defining the engine contract
       - BaseSQLEngine: Synchronous SQL query execution
    
    2. **Platform Implementations**: Concrete implementations for each platform
       - Synapse: SynapseSQLEngine

Engine Types:
    **SQL Engines**:
        - Execute SQL queries synchronously
        - Return results as DataFrames, scalars, or success indicators
        - Handle connection pooling and retries
        - Support parameterized queries for security
    
    **Spark Engines**:
        - Submit Spark jobs asynchronously
        - Monitor job status and progress
        - Manage Spark session configuration
        - Handle job cancellation and timeouts

Engine Responsibilities:
    - Query/job execution with proper error handling
    - Connection lifecycle management (create, reuse, cleanup)
    - Retry logic for transient failures
    - Performance monitoring and metrics collection
    - Resource cleanup on completion or failure
    - Platform-specific optimizations

Engines do NOT handle:
    - Query generation (see query_builder module)
    - Table management logic (see processors module)
    - Platform selection (see platforms module)
    - Business logic or data transformations

Security Features:
    - Parameterized query support to prevent SQL injection
    - Secure credential management via Azure Identity
    - Connection encryption and secure protocols
    - Audit logging for compliance

Example Usage:
    >>> # Engines are managed internally by platforms
    >>> from medalflow.compute import create_platform, ExecuteSQL
    >>> from medalflow.constants.compute import ResultFormat
    >>> 
    >>> platform = create_platform()
    >>> 
    >>> # Execute operations through the platform
    >>> # Engines are not directly accessible
    >>> 
    >>> # Execute a DDL query
    >>> op = ExecuteSQL(
    ...     sql="CREATE SCHEMA IF NOT EXISTS bronze",
    ...     schema_name="dbo",
    ...     object_name="bronze"
    ... )
    >>> result = platform.execute(op)
    >>> 
    >>> # Fetch results as DataFrame (default)
    >>> op = ExecuteSQL(
    ...     sql="SELECT * FROM bronze.customers WHERE region = 'US'",
    ...     returns_results=True,
    ...     schema_name="bronze",
    ...     object_name="customers"
    ... )
    >>> result = platform.execute(op)
    >>> df = result.data  # pandas DataFrame
    >>> 
    >>> # Get scalar value
    >>> op = ExecuteSQL(
    ...     sql="SELECT COUNT(*) FROM bronze.customers",
    ...     returns_results=True,
    ...     result_format=ResultFormat.SCALAR,
    ...     schema_name="bronze",
    ...     object_name="customers"
    ... )
    >>> result = platform.execute(op)
    >>> count = result.data  # scalar value

Connection Management:
    Engines implement connection pooling and reuse to minimize overhead.
    Connections are managed internally by the platform and engines.

Error Handling:
    Platforms provide structured error handling through OperationResult:
    
    >>> op = ExecuteSQL(
    ...     sql="SELECT * FROM invalid_table",
    ...     returns_results=True,
    ...     schema_name="dbo",
    ...     object_name="invalid_table"
    ... )
    >>> result = platform.execute(op)
    >>> if not result.success:
    ...     print(f"Query failed: {result.error_message}")
    ...     print(f"Error type: {result.error_type}")

Performance Considerations:
    - Connection pooling reduces connection overhead
    - Batch operations minimize round trips
    - Result streaming for large datasets
    - Query timeout configuration

See Also:
    - medalflow.compute.engines.base: Base engine interfaces
    - medalflow.compute.engines.synapse: Azure Synapse implementations
    - medalflow.compute.platforms: Platform orchestration layer
"""

from medalflow.compute.engines.base import BaseSQLEngine

__all__ = ["BaseSQLEngine"]