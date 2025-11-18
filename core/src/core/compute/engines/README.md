# Compute Engines (INTERNAL)

> **⚠️ WARNING: This module is INTERNAL to the compute package and should NEVER be accessed directly from outside. All external interactions MUST go through the Platform abstraction layer.**

The engines module provides the internal execution layer for database operations, handling the actual communication with compute platforms through SQL and Spark interfaces.

## Overview

Engines are internal components responsible for executing queries and managing connections to the underlying compute platforms. They are completely encapsulated by the Platform layer and are not part of the public API. The module provides a base implementation using SQLAlchemy that works across all platforms, with minimal platform-specific customization.

**Key Points:**
- Engines are INTERNAL implementation details
- NEVER import or use engines directly from outside the compute module
- Platforms provide the ONLY public interface to engine functionality
- Engine interfaces may change without notice as they are not public API

## Architecture

```
engines/
├── base.py           # Base engine interfaces and SQLAlchemy implementation
├── synapse/          # Azure Synapse-specific engines
│   ├── sql_engine.py
│   └── spark_engine.py
└── fabric/           # Microsoft Fabric-specific engines
    ├── sql_engine.py
    └── spark_engine.py
```

## Engine Types

### SQL Engines

SQL engines handle synchronous query execution for DDL and DML operations. They use SQLAlchemy with ODBC connections for maximum compatibility.

**Key Features:**
- Connection pooling for performance
- Automatic retry with exponential backoff
- Multiple result formats (DataFrame, dict_list, scalar)
- Batch execution support
- Platform-specific optimizations

### Spark Engines

Spark engines manage distributed processing jobs for large-scale data operations. They operate asynchronously with job submission and monitoring capabilities.

**Key Features:**
- Asynchronous job submission
- Job status monitoring
- Resource configuration
- Result retrieval
- Job cancellation support

## Base Implementation (Internal Use Only)

### BaseSQLEngine

The `BaseSQLEngine` class provides a complete SQLAlchemy-based implementation that works with any ODBC-compatible platform. This is used internally by platform implementations:

```python
# INTERNAL: Example for platform developers only
# This code would be in platforms/{platform}/sql_engine.py
from core.compute.engines.base import BaseSQLEngine
from core.settings import BaseComputeSettings

class CustomSQLEngine(BaseSQLEngine):
    """Internal SQL engine for custom platform."""
    
    def _apply_connection_settings(self, conn):
        """Apply platform-specific SET commands."""
        conn.execute(text("SET CUSTOM_OPTION ON"))
```

**Core Methods:**
- `execute_query()`: Execute DDL/DML without results
- `fetch_dataframe()`: Return results as pandas DataFrame
- `fetch_scalar()`: Return single value
- `fetch_all()`: Return list of dictionaries
- `execute_batch()`: Execute multiple queries efficiently

### BaseSparkEngine

The `BaseSparkEngine` abstract class defines the internal interface for Spark job management:

```python
# INTERNAL: Example for platform developers only
# This code would be in platforms/{platform}/spark_engine.py
from core.compute.engines.base import BaseSparkEngine
from core.compute.types import SparkJobConfig, JobResult

class CustomSparkEngine(BaseSparkEngine):
    """Internal Spark engine for custom platform."""
    
    def submit_job(self, config: SparkJobConfig) -> str:
        """Submit Spark job for execution."""
        # Implementation
        
    def get_job_status(self, job_id: str) -> JobStatus:
        """Check job status."""
        # Implementation
```

## Platform Implementations

### Synapse Engines

#### SynapseSQLEngine (Internal)

Minimal customization of BaseSQLEngine for Azure Synapse. This is used internally by `SynapsePlatform`:

```python
# INTERNAL: This is how SynapsePlatform uses the engine internally
# External code should NEVER access engines directly

class SynapsePlatform(_BasePlatform):
    def _initialize_dependencies(self):
        # Platform creates and manages engine internally
        self._sql_engine = SynapseSQLEngine(self.settings, self.environment)
```

**Synapse-Specific Features:**
- Required SET options for proper operation
- External table support
- Serverless pool optimization

#### SynapseSparkEngine

Manages Spark jobs in Synapse Spark pools (when implemented):
- Job submission to Spark pools
- Livy API integration
- Status monitoring

### Fabric Engines

#### FabricSQLEngine (Internal)

Optimized for Microsoft Fabric's warehouse and lakehouse. This is used internally by `FabricPlatform`:

```python
# INTERNAL: This is how FabricPlatform uses the engine internally
# External code should NEVER access engines directly

class FabricPlatform(_BasePlatform):
    def _initialize_dependencies(self):
        # Platform creates and manages engine internally
        self._sql_engine = FabricSQLEngine(self.settings, self.environment)
```

**Fabric-Specific Features:**
- Native Delta Lake support
- Direct Lake mode compatibility
- Managed table operations

#### FabricSparkEngine

Handles Spark workloads in Fabric (when implemented):
- Notebook execution
- Spark job submission
- Resource management

## Connection Management

### SQLAlchemy Configuration

All SQL engines use SQLAlchemy with configurable connection pooling:

```python
# Configuration via settings
sql_pool_size = 5           # Number of persistent connections
sql_max_overflow = 10        # Maximum overflow connections
sql_pool_timeout = 30        # Connection timeout in seconds
```

### ODBC Connection Strings

Engines retrieve ODBC strings from platform settings:

```python
# Synapse ODBC format
"DRIVER={ODBC Driver 18 for SQL Server};SERVER=...;DATABASE=...;UID=...;PWD=..."

# Fabric ODBC format  
"DRIVER={ODBC Driver 18 for SQL Server};SERVER=...;DATABASE=...;Authentication=..."
```

## Error Handling

### Retry Logic

Built-in retry with exponential backoff for transient failures:

```python
@retry(max_retries=3, initial_delay=1, exponential_base=2)
def execute_query(self, query: str):
    """Automatically retries on transient failures."""
    # Implementation
```

### Exception Types

- `ConnectionError`: Connection establishment failures
- `QueryExecutionError`: Query execution failures
- `JobSubmissionError`: Spark job submission failures
- `JobNotFoundError`: Job tracking failures

## Performance Optimizations

### Connection Pooling

- Pre-allocated connection pool for reduced latency
- Connection validation before use (`pool_pre_ping`)
- Automatic connection recycling

### Query Execution

- Prepared statements where supported
- Batch execution for multiple queries
- Streaming for large result sets

### Result Handling

- Efficient DataFrame creation with pandas
- Memory-efficient dictionary conversion
- Scalar optimization for single values

## Internal Usage by Platforms

### How Platforms Use Engines Internally

```python
# INTERNAL: This shows how platforms use engines internally
# External code should NEVER do this - use platforms instead!

class _BasePlatform:
    def _execute_with_sql(self, query: str, operation: BaseOperation):
        """Internal method showing engine usage."""
        # Platform manages engine internally
        engine = self._get_sql_engine()
        
        # Platform handles all engine interactions
        if operation.returns_results:
            data = engine.fetch_dataframe(query)
        else:
            engine.execute_query(query)
        
        # Platform returns results to external code
        return OperationResult(...)
```

### Correct External Usage (Through Platforms)

```python
# CORRECT: External code uses platforms, not engines
from core.compute import PlatformFactory

platform = PlatformFactory.create()

# Platform handles all engine management internally
result = platform.execute_sql_query(
    "SELECT COUNT(*) FROM silver.products",
    result_format=ResultFormat.SCALAR
)

# Or use operations
from core.operations import CreateTable

operation = CreateTable(schema="silver", object_name="products", ...)
result = platform.execute_operation(operation)
```

## Testing (Internal Platform Tests)

```python
# INTERNAL: How platform implementations test their engines
# This is for platform developers, not external users

class TestSynapsePlatform:
    def test_platform_engine_integration(self):
        """Test that platform correctly uses its internal engine."""
        platform = SynapsePlatform(settings)
        
        # Test through platform interface, not engine directly
        connections = platform.test_connection()
        assert connections['sql'] == True
        
        # Verify platform operations work
        result = platform.execute_sql_query("SELECT 1")
        assert result == 1
```

## Best Practices for Platform Developers

> **For External Users**: Use platforms, not engines. See the [platforms README](../platforms/README.md).

1. **Engine Encapsulation**: Keep engines private within platform implementations
2. **Connection Management**: Let SQLAlchemy handle connection pooling
3. **Error Propagation**: Convert engine exceptions to platform-level errors
4. **Resource Cleanup**: Properly dispose of engines in platform cleanup
5. **Testing**: Test engines through platform interfaces, not directly

## Extending Engines (For Platform Developers)

### Adding a New SQL Engine (Internal)

When creating a new platform, you'll need to create internal engine implementations:

1. Create engine in `engines/{platform}/sql_engine.py`
2. Inherit from `BaseSQLEngine`
3. Override methods only if platform-specific behavior is needed
4. Use engine ONLY within your platform implementation

```python
# File: engines/newplatform/sql_engine.py
# INTERNAL: Not exposed to external users

from core.compute.engines.base import BaseSQLEngine

class NewPlatformSQLEngine(BaseSQLEngine):
    """Internal SQL engine for new platform."""
    
    def _apply_connection_settings(self, conn):
        """Apply platform-specific settings."""
        # Add any required SET commands
        pass
```

Then use it internally in your platform:

```python
# File: platforms/newplatform.py

class NewPlatform(_BasePlatform):
    def _initialize_dependencies(self):
        # Engine is created and managed internally
        self._sql_engine = NewPlatformSQLEngine(self.settings)
```

### Adding a New Spark Engine

1. Implement `BaseSparkEngine` interface
2. Handle job submission and monitoring
3. Integrate with platform's Spark API

```python
from core.compute.engines.base import BaseSparkEngine

class NewPlatformSparkEngine(BaseSparkEngine):
    """Spark engine for new platform."""
    
    def submit_job(self, config):
        """Submit job to platform's Spark cluster."""
        # Implementation
    
    def get_job_status(self, job_id):
        """Query job status from platform."""
        # Implementation
```

## Configuration Reference

### Environment Variables

```bash
# Connection pool settings
CTE_COMPUTE__SQL_POOL_SIZE=5
CTE_COMPUTE__SQL_MAX_OVERFLOW=10
CTE_COMPUTE__SQL_POOL_TIMEOUT=30

# Platform-specific ODBC strings
CTE_COMPUTE__SYNAPSE__CONNECTION__ODBC_STRING_ETL="..."
CTE_COMPUTE__FABRIC__CONNECTION__ODBC_STRING_ETL="..."
```

### Settings Classes

- `BaseComputeSettings`: Common engine configuration
- `SynapseSettings`: Synapse-specific settings
- `FabricSettings`: Fabric-specific settings

## See Also

- [`base.py`](./base.py) - Base engine implementations
- [`../platforms/`](../platforms/README.md) - Platform implementations
- [`core.settings`](../../settings/README.md) - Configuration details
- [`core.query_builder`](../../query_builder/README.md) - Query generation