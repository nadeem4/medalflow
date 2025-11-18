# Compute Module

The compute module provides platform-agnostic abstractions for data processing operations across multiple cloud compute platforms, including Azure Synapse Analytics and Microsoft Fabric.

## Overview

The compute module implements an **operation-based architecture** where all database operations are represented as data classes. Platforms handle the execution details internally, maintaining complete platform independence for the medallion layer and other consumers.

### Key Concepts

- **Operations as Data**: All database operations (CREATE, INSERT, UPDATE, DELETE, MERGE) are represented as immutable data classes
- **Platform Abstraction**: Platforms provide the ONLY public interface - they completely encapsulate engine complexity
- **Internal Engine Management**: Platforms internally manage SQL and Spark engines (not exposed to consumers)
- **Batch Processing**: Execute multiple operations efficiently with optional transaction support

## Architecture

```
compute/
├── platforms/       # Platform implementations (PUBLIC API)
├── engines/        # INTERNAL: Execution engines (SQL, Spark)
├── factory.py      # Platform factory for instantiation
├── types.py        # Result types and configurations
└── __init__.py     # Public API exports
```

### Layers

1. **Operations Layer**: Data classes representing database operations (from `core.operations`)
2. **Platform Layer**: PUBLIC API - Manages platform-specific execution logic (`platforms/`)
3. **Engine Layer**: INTERNAL - Handles actual query execution (`engines/`) - Never access directly
4. **Query Builder Layer**: Generates platform-specific SQL (from `core.query_builder`)
5. **Factory Layer**: Creates platform instances (`factory.py`)

> **Important**: The Engine Layer is an internal implementation detail and should NEVER be accessed directly. All interactions must go through the Platform Layer, which provides the public abstraction.

## Configuration

The compute module is configured through environment variables with the `CTE_COMPUTE__` prefix:

```python
# Key settings
CTE_COMPUTE__COMPUTE_TYPE=synapse          # Platform type (synapse/fabric)
CTE_COMPUTE__SYNAPSE__CONNECTION__ENDPOINT=... # Synapse SQL endpoint
CTE_COMPUTE__FABRIC__WORKSPACE__ID=...     # Fabric workspace ID
```

See `core.settings.ComputeSettings` for complete configuration options.

## Usage Examples

### Basic Operations

```python
from core.compute import PlatformFactory, CreateTable, Insert

# Get platform instance (configured via settings)
platform = PlatformFactory.create()

# Create a table
create_op = CreateTable(
    schema="silver",
    object_name="customers",
    select_query="SELECT * FROM bronze.raw_customers WHERE active = 1"
)
result = platform.execute_operation(create_op)
print(f"Table created: {result.success}")

# Insert data
insert_op = Insert(
    schema="silver",
    object_name="customers",
    source_query="SELECT * FROM staging.new_customers"
)
result = platform.execute_operation(insert_op)
print(f"Rows inserted: {result.rows_affected}")
```

### Direct SQL Execution (for APIs)

```python
# Optimized for REST APIs and Azure Functions
from core.compute import PlatformFactory, ResultFormat

platform = PlatformFactory.create()

# Return JSON-serializable results with metadata
result = platform.execute_sql_query(
    "SELECT * FROM customers WHERE city = 'Seattle'",
    result_format=ResultFormat.DICT_LIST
)
if result.success:
    customers = result.data  # List of dictionaries
    print(f"Found {result.rows_affected} customers in {result.duration_seconds:.2f}s")
else:
    print(f"Query failed: {result.error_message}")

# Get scalar value with error handling
result = platform.execute_sql_query(
    "SELECT COUNT(*) FROM orders",
    result_format=ResultFormat.SCALAR
)
if result.success:
    count = result.data  # Single value
    print(f"Order count: {count}")
```

### Batch Operations

```python
from core.compute import (
    PlatformFactory, CreateTable, Insert, 
    CreateStatistics, CreateOrAlterView
)

platform = PlatformFactory.create()

# Define multiple operations
operations = [
    CreateTable(schema="silver", object_name="products", ...),
    Insert(schema="silver", object_name="products", ...),
    CreateStatistics(schema="silver", object_name="products", columns=["id"]),
    CreateOrAlterView(schema="silver", object_name="v_products", ...)
]

# Execute as batch with transaction
batch_result = platform.execute_batch(
    operations, 
    transaction=True,
    stop_on_error=True
)

print(f"Success rate: {batch_result.success_rate}%")
for i, result in enumerate(batch_result.results):
    if not result.success:
        print(f"Operation {i} failed: {result.error_message}")
```

### Complex Operations

```python
from core.compute import PlatformFactory, Merge

platform = PlatformFactory.create()

# Upsert operation
merge_op = Merge(
    schema="silver",
    object_name="customers",
    source_query="SELECT * FROM staging.customer_updates",
    merge_columns=["customer_id"],
    update_columns=["name", "email", "updated_at"],
    insert_columns=["customer_id", "name", "email", "created_at", "updated_at"]
)
result = platform.execute_operation(merge_op)
print(f"Merge completed: {result.rows_affected} rows affected")
```

## Platform Support

### Currently Supported
- **Azure Synapse Analytics**: Serverless and dedicated SQL pools with external table support
- **Microsoft Fabric**: Native lakehouse integration with managed tables

### Internal Engine Types (Managed by Platforms)
- **SQL Engine**: Internal - Synchronous query execution via SQLAlchemy/ODBC
- **Spark Engine**: Internal - Asynchronous job submission for large-scale processing

> **Note**: Engines are internal implementation details. Consumers interact only with platforms.

## Key Features

### Operation Types
- **DDL Operations**: CreateTable, DropTable, CreateSchema, CreateOrAlterView
- **DML Operations**: Insert, Update, Delete, Merge, Copy
- **Utility Operations**: CreateStatistics, ExecuteSQL

### Performance Optimizations
- Connection pooling (internally managed)
- Intelligent engine selection (handled internally by platforms)
- Batch execution for multiple operations
- Optimized result formats (scalar, dataframe, dict_list)

### Error Handling
- Built-in retry logic with exponential backoff
- Comprehensive error reporting with operation context
- Transaction support with rollback on failure

## Design Principles

1. **Operations as Data**: All operations are immutable data classes, making them easy to test, serialize, and reason about
2. **Platform Independence**: The medallion layer and other consumers don't need to know about platform specifics
3. **Fail-Safe Defaults**: Intelligent defaults with clear override options
4. **Performance First**: Optimized for both single operations and batch processing
5. **Extensibility**: Easy to add new platforms through the factory pattern

## Testing

```python
# Test platform connections
platform = PlatformFactory.create()
connections = platform.test_connection()
print(f"SQL Engine: {connections.get('sql', False)}")
print(f"Spark Engine: {connections.get('spark', False)}")

# Get platform information
info = platform.get_info()
print(f"Platform: {info['name']}")
print(f"Supported Engines: {info['supported_engines']}")
print(f"Transaction Support: {info['supports_transactions']}")
```

## Best Practices

1. **Use Operations**: Prefer operation objects over direct SQL for maintainability
2. **Batch When Possible**: Group related operations for better performance
3. **Handle Errors**: Always check `result.success` and handle failures appropriately
4. **Configure Properly**: Ensure settings are properly configured for your environment
5. **Use Type Hints**: Leverage type hints for better IDE support and fewer runtime errors

## Advanced Topics

### Custom Platform Implementation

To add support for a new platform:

1. Create platform implementation in `platforms/` (public interface)
2. Create internal engine implementations in `engines/{platform}/` (internal only)
3. Create query builder in `core.query_builder/{platform}/`
4. Register with factory in `factory.py`

> **Important**: Only the platform class is public. Engines remain internal.

### Internal Engine Selection

Platforms internally manage engine selection based on:
- Operation type (DDL/DML)
- Data volume estimates
- Engine availability
- Explicit hints via `engine_hint` parameter

> **Note**: This is handled internally by platforms. Consumers don't interact with engines directly.

### Transaction Support

Transactions are supported where the platform allows:
- Synapse: Limited transaction support
- Fabric: Full transaction support in warehouse mode

## See Also

- [`platforms/`](./platforms/README.md) - Platform implementations (PUBLIC API)
- [`engines/`](./engines/README.md) - Internal engine implementations (INTERNAL ONLY)
- [`core.operations`](../operations/README.md) - Operation definitions
- [`core.query_builder`](../query_builder/README.md) - Query builders
- [`core.settings`](../settings/README.md) - Configuration settings