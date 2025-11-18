# Compute Platforms (PUBLIC API)

The platforms module provides the **PUBLIC abstraction layer** that enables platform-agnostic data processing across different cloud compute services. This is the ONLY interface that external code should use to interact with the compute module.

## Overview

Platforms are the **public-facing orchestrators** in the compute module. They provide a clean, stable API that completely encapsulates all internal implementation details including engines, connections, and query generation. Platforms receive operation objects, internally manage engine selection and execution, and return results. This design allows the medallion layer and other consumers to work with a unified interface without any knowledge of the underlying infrastructure.

**Key Points:**
- Platforms are the ONLY public API for compute operations
- Engines are completely hidden internal implementation details
- External code NEVER directly accesses engines, query builders, or connections
- Platform interfaces are stable and backward-compatible

## Architecture

```
platforms/
├── base.py       # BasePlatform abstract class with common logic
├── synapse.py    # Azure Synapse Analytics implementation
└── fabric.py     # Microsoft Fabric implementation
```

## Core Concepts

### Platform Responsibilities

1. **Public API**: Provide the ONLY external interface to compute functionality
2. **Operation Execution**: Process operation objects and return results
3. **Internal Engine Management**: Internally create, manage, and dispose of engines
4. **Query Generation**: Internally delegate to platform-specific query builders
5. **Batch Processing**: Execute multiple operations efficiently
6. **Transaction Management**: Handle transactions where supported
7. **Complete Encapsulation**: Hide all implementation details from external code

### Operation Flow

```
EXTERNAL CODE
    ↓
Operation Object
    ↓
Platform (PUBLIC API)
━━━━━━━━━━━━━━━━━━━━━━━━  ← API Boundary (nothing below is public)
    ↓
[INTERNAL] Engine Selection
    ↓
[INTERNAL] Query Builder
    ↓
[INTERNAL] Engine Execution
━━━━━━━━━━━━━━━━━━━━━━━━
    ↓
Result Object
    ↓
EXTERNAL CODE
```

## Base Platform

The `_BasePlatform` class provides the foundation for all platform implementations:

```python
from core.compute.platforms.base import _BasePlatform
from core.constants.compute import ComputeEnvironment

class CustomPlatform(_BasePlatform):
    """Custom platform implementation (public interface)."""
    
    def name(self) -> str:
        return "custom"
    
    def supported_engines(self) -> List[EngineType]:
        # These are internal capabilities, not exposed directly
        return [EngineType.SQL, EngineType.SPARK]
    
    def _initialize_dependencies(self) -> None:
        """Initialize internal components (not visible to external code)."""
        # All these are internal - external code never sees them
        self._sql_engine = CustomSQLEngine(self.settings)  # Internal
        self._spark_engine = CustomSparkEngine(self.settings)  # Internal
        self._query_builder = CustomQueryBuilder()  # Internal
```

### Public API Methods

These are the ONLY methods external code should use:

- `execute_operation()`: Execute a single operation
- `execute_batch()`: Execute multiple operations
- `execute_sql_query()`: Direct SQL execution for APIs
- `test_connection()`: Verify platform connectivity
- `get_info()`: Retrieve platform configuration

All other methods (prefixed with `_`) are internal and should NEVER be called from outside.

## Platform Implementations

### SynapsePlatform

Azure Synapse Analytics platform with external table support:

```python
from core.compute.platforms.synapse import SynapsePlatform
from core.settings import SynapseSettings

settings = SynapseSettings(...)
platform = SynapsePlatform(settings, ComputeEnvironment.ETL)

# Synapse-specific features
# - External tables for data lake integration
# - Serverless SQL pool support
# - Optional Spark pool integration
```

**Key Features:**
- External table creation for Bronze/Silver/Gold layers
- OPENROWSET support for serverless queries
- Distribution strategies (HASH, ROUND_ROBIN)
- Statistics creation for query optimization

### FabricPlatform

Microsoft Fabric platform with native lakehouse integration:

```python
from core.compute.platforms.fabric import FabricPlatform
from core.settings import FabricSettings

settings = FabricSettings(...)
platform = FabricPlatform(settings, ComputeEnvironment.ETL)

# Fabric-specific features
# - Managed Delta Lake tables
# - Direct Lake mode support
# - Integrated lakehouse/warehouse
```

**Key Features:**
- Native Delta Lake format
- Managed tables (no external tables needed)
- Direct Lake for Power BI integration
- Seamless lakehouse/warehouse switching

## Internal Engine Management

Platforms internally manage engine selection, but this is completely hidden from external code:

```python
# INTERNAL: This is how platforms work internally
# External code should NOT know or care about this

def _select_engine_for_operation(self, operation: BaseOperation) -> EngineType:
    """Internal method - not part of public API."""
    # Platform handles all engine selection internally
    # External code just calls execute_operation()
    # and gets results back
    ...
```

**For external code**: Simply pass operations to the platform. The platform handles everything internally.

## Public API Usage

### Basic Operation Execution (Correct Usage)

```python
from core.compute import PlatformFactory
from core.operations import CreateTable, Insert

# Get configured platform
platform = PlatformFactory.create()

# Create table operation
create_op = CreateTable(
    schema="silver",
    object_name="customers",
    select_query="SELECT * FROM bronze.raw_customers"
)

# Execute and get result
result = platform.execute_operation(create_op)
if result.success:
    print(f"Table created in {result.duration_seconds:.2f} seconds")
else:
    print(f"Error: {result.error_message}")
```

### Batch Operations with Transactions (Correct Usage)

```python
from core.operations import Update, Delete, CreateStatistics

operations = [
    Update(
        schema="silver",
        object_name="orders",
        update_columns={"status": "'processed'"},
        where_clause="order_date < DATEADD(day, -30, GETDATE())"
    ),
    Delete(
        schema="silver",
        object_name="temp_staging",
        where_clause="1=1"
    ),
    CreateStatistics(
        schema="silver",
        object_name="orders",
        columns=["customer_id", "order_date"]
    )
]

# Execute with transaction (if supported)
batch_result = platform.execute_batch(
    operations,
    transaction=True,
    stop_on_error=True
)

print(f"Executed {batch_result.total_operations} operations")
print(f"Success rate: {batch_result.success_rate}%")
```

### Direct SQL for APIs

```python
from core.constants.compute import ResultFormat

# For REST API endpoints
@app.route("/api/customers/<city>")
def get_customers(city):
    platform = get_platform()
    
    # Returns OperationResult with JSON-serializable data
    result = platform.execute_sql_query(
        f"SELECT * FROM customers WHERE city = '{city}'",
        result_format=ResultFormat.DICT_LIST
    )
    
    if result.success:
        return jsonify({
            "data": result.data,  # List of dictionaries
            "count": result.rows_affected,
            "duration_ms": result.duration_seconds * 1000
        })
    else:
        return jsonify({"error": result.error_message}), 500

# For Azure Functions
def main(req: func.HttpRequest) -> func.HttpResponse:
    platform = get_platform()
    
    # Get scalar value with full metadata
    result = platform.execute_sql_query(
        "SELECT COUNT(*) FROM orders WHERE status = 'pending'",
        result_format=ResultFormat.SCALAR
    )
    
    if result.success:
        return func.HttpResponse(
            f"Pending orders: {result.data} (query took {result.duration_seconds:.2f}s)"
        )
    else:
        return func.HttpResponse(
            f"Error: {result.error_message}",
            status_code=500
        )
```

### Platform Information

```python
# Test connections
platform = PlatformFactory.create()
connections = platform.test_connection()

for engine, status in connections.items():
    print(f"{engine}: {'✓' if status else '✗'}")

# Get platform details
info = platform.get_info()
print(f"Platform: {info['name']}")
print(f"Engines: {', '.join(info['supported_engines'])}")
print(f"Transactions: {info['supports_transactions']}")
```

## Transaction Support

Platforms provide transaction support where available:

```python
# Check transaction support
if platform._supports_transactions():
    platform._begin_transaction()
    try:
        # Execute operations
        platform.execute_operation(op1)
        platform.execute_operation(op2)
        platform._commit_transaction()
    except:
        platform._rollback_transaction()
        raise
```

**Platform Support:**
- **Synapse**: Limited transaction support
- **Fabric**: Full transaction support in warehouse mode

## Extending Platforms

### Adding a New Platform

1. Create platform class inheriting from `_BasePlatform`
2. Implement required abstract methods
3. Initialize platform-specific dependencies
4. Register with factory

```python
from core.compute.platforms.base import _BasePlatform
from core.compute.engines.custom import CustomSQLEngine
from core.query_builder.custom import CustomQueryBuilder

class CustomPlatform(_BasePlatform):
    """Custom cloud platform implementation."""
    
    def name(self) -> str:
        """Platform identifier."""
        return "custom"
    
    def supported_engines(self) -> List[EngineType]:
        """Available engines for this platform."""
        return [EngineType.SQL, EngineType.SPARK, EngineType.AUTO]
    
    def _initialize_dependencies(self) -> None:
        """Set up platform components."""
        # Initialize SQL engine
        self._sql_engine = CustomSQLEngine(
            self.settings,
            self.environment
        )
        
        # Initialize query builder
        self._query_builder = CustomQueryBuilder()
        
        # Initialize Spark if available
        if self.settings.spark_configured:
            self._spark_engine = CustomSparkEngine(
                self.settings,
                self.environment
            )
    
    def _supports_transactions(self) -> bool:
        """Check transaction support."""
        return True  # If platform supports transactions
```

### Platform-Specific Features

Override base methods to add platform-specific behavior:

```python
def _execute_with_sql(self, query: str, operation: BaseOperation) -> OperationResult:
    """Add platform-specific execution logic."""
    # Custom pre-processing
    if self._requires_special_handling(operation):
        query = self._transform_query(query)
    
    # Execute with base implementation
    result = super()._execute_with_sql(query, operation)
    
    # Custom post-processing
    if operation.operation_type == QueryType.CREATE_TABLE:
        self._create_table_statistics(operation)
    
    return result
```

## Performance Considerations

### Engine Selection
- SQL for small to medium operations
- Spark for large-scale transformations
- Consider data volume and complexity

### Batch Processing
- Group related operations
- Use transactions where available
- Consider connection pooling limits

### Query Optimization
- Platform-specific query builders optimize SQL
- Statistics creation for better query plans
- Distribution strategies for large tables

## Best Practices

1. **Use Platform Factory**: Always create platforms through the factory
2. **Handle Results**: Check `result.success` before proceeding
3. **Batch Operations**: Group related operations for efficiency
4. **Resource Cleanup**: Call `platform.cleanup()` when done
5. **Error Handling**: Log operation failures with context
6. **Environment Selection**: Use appropriate environment (ETL vs CONSUMPTION)

## Configuration

### Environment Variables

```bash
# Platform selection
CTE_COMPUTE__COMPUTE_TYPE=synapse  # or fabric

# Synapse configuration
CTE_COMPUTE__SYNAPSE__CONNECTION__ENDPOINT=...
CTE_COMPUTE__SYNAPSE__LAKE_DATABASE_NAME=...

# Fabric configuration  
CTE_COMPUTE__FABRIC__WORKSPACE__ID=...
CTE_COMPUTE__FABRIC__LAKE_DATABASE_NAME=...
```

### Settings Classes

- `ComputeSettings`: Top-level compute configuration
- `SynapseSettings`: Synapse-specific settings
- `FabricSettings`: Fabric-specific settings

## Testing Platforms

```python
import pytest
from unittest.mock import Mock

def test_platform_execution():
    """Test platform executes operations correctly."""
    # Mock dependencies
    mock_engine = Mock()
    mock_builder = Mock()
    
    # Create platform with mocks
    platform = TestPlatform()
    platform._sql_engine = mock_engine
    platform._query_builder = mock_builder
    
    # Test operation execution
    operation = CreateTable(...)
    result = platform.execute_operation(operation)
    
    # Verify interactions
    mock_builder.build_query.assert_called_once_with(operation)
    mock_engine.execute_query.assert_called_once()
    assert result.success
```

## Troubleshooting

### Connection Issues
- Verify ODBC connection strings
- Check firewall rules
- Validate authentication

### Performance Issues
- Review engine selection logic
- Check connection pool settings
- Monitor query execution times

### Transaction Failures
- Verify platform transaction support
- Check for DDL/DML mixing
- Review error logs for deadlocks

## See Also

- [`base.py`](./base.py) - Base platform implementation
- [`../engines/`](../engines/README.md) - Engine implementations
- [`../factory.py`](../factory.py) - Platform factory
- [`core.operations`](../../operations/README.md) - Operation definitions
- [`core.query_builder`](../../query_builder/README.md) - Query builders