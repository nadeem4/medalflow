# Query Builder Module

## ⚠️ Internal Module - Not for External Use

**IMPORTANT**: This module is an internal component of the MedalFlow framework and is NOT part of the public API. It should not be used directly outside of the medalflow package. All database operations should go through the public API in `core.api`.

## Overview

The `query_builder` module provides platform-specific SQL generation capabilities for the MedalFlow framework. It translates high-level operation objects into platform-specific SQL statements while ensuring security through comprehensive input validation and SQL injection protection.

### Key Design Principles

1. **SQL Generation Only**: Query builders generate SQL strings but do NOT execute queries
2. **Platform-Specific**: Each platform has its own SQL dialect and features
3. **Security First**: All inputs are validated to prevent SQL injection
4. **Stateless**: Builders don't maintain state between calls
5. **Operation-Based**: All builders work with strongly-typed Operation objects

## Architecture

The query builder module sits in **Layer 1 (Infrastructure)** of the MedalFlow architecture, providing fundamental SQL generation capabilities that can be used by multiple Layer 2 business logic modules.

```
Layer 2: Business Logic
    ├── compute (uses query builders)
    ├── medallion (uses query builders)
    └── datalake
           ↓
Layer 1: Infrastructure
    ├── query_builder (THIS MODULE)
    ├── operations (data structures)
    └── settings
           ↓
Layer 0: Constants & Types
```

### Why Layer 1?

- **Shared Infrastructure**: Query builders are used by multiple Layer 2 modules (compute, medallion)
- **No Business Logic**: Pure SQL generation without business rules
- **Foundation Service**: Provides fundamental capabilities for higher layers

## Module Structure

```
query_builder/
├── __init__.py           # Module exports and documentation
├── base.py              # Abstract base class with security features
├── factory.py           # Factory for creating configured builders
├── synapse/
│   ├── __init__.py
│   └── serverless_builder.py  # Synapse Serverless SQL implementation
└── fabric/
    ├── __init__.py
    └── warehouse_builder.py    # Fabric Warehouse implementation
```

## Core Components

### BaseQueryBuilder (Abstract Base Class)

The foundation for all platform-specific builders. Accepts a `_Settings` object in the constructor which provides all necessary configuration including table prefixes and schema-specific rules.

- **Security Features**:
  - Identifier validation (schema, table, column names)
  - SQL injection protection through regex validation
  - Safe string escaping and quoting
  - Maximum length constraints on identifiers

- **Common Methods** (Available to all implementations):
  - `build_query(operation)`: Main entry point for SQL generation
  - `fully_qualified_name(schema, object_name)`: Builds [schema].[prefixed_object_name] with proper quoting
  - `quote_identifier(identifier)`: Platform-specific identifier quoting (strips existing brackets to prevent double-quoting)
  - `quote_string(value)`: Safely quotes and escapes string values
  - `format_column_list(columns)`: Formats list of columns with proper quoting
  - `format_value_list(values)`: Formats list of values for INSERT/VALUES
  - `format_set_clause(columns)`: Formats SET clause for UPDATE statements
  - `format_column_definitions(columns)`: Formats column definitions for CREATE TABLE
  - `build_select_all(schema, object_name)`: Builds SELECT * query
  - `build_select_columns(schema, object_name, columns)`: Builds SELECT with specific columns
  - `build_select_where(schema, object_name, where_clause, columns)`: Builds SELECT with WHERE
  - `build_select_where_not(schema, object_name, where_clause, columns)`: Builds SELECT with WHERE NOT

- **Protected/Internal Methods** (For use within builders):
  - `_validate_identifier(identifier, identifier_type)`: Validates identifiers for SQL injection
  - `_is_expression(value)`: Checks if a string is a SQL expression
  - `_validate_sql_expression(expression, expression_type)`: Validates SQL expressions

- **Abstract Methods**: Each platform must implement all 14 methods:
  - `_build_create_table()` - Generate CREATE TABLE statements
  - `_build_drop_table()` - Generate DROP TABLE statements
  - `_build_insert()` - Generate INSERT statements
  - `_build_update()` - Generate UPDATE statements
  - `_build_delete()` - Generate DELETE statements
  - `_build_merge()` - Generate MERGE/UPSERT statements
  - `_build_copy()` - Generate COPY INTO statements
  - `_build_create_or_alter_view()` - Generate CREATE OR ALTER VIEW statements
  - `_build_drop_view()` - Generate DROP VIEW statements
  - `_build_create_statistics()` - Generate CREATE STATISTICS statements
  - `_build_create_schema()` - Generate CREATE SCHEMA statements
  - `_build_drop_schema()` - Generate DROP SCHEMA statements
  - `_build_select()` - Generate SELECT statements
  - `_build_execute_sql()` - Validate and return arbitrary SQL statements

### Platform Implementations

#### SynapseServerlessQueryBuilder

Optimized for Azure Synapse Serverless SQL pools:

**Features**:
- External table creation with PolyBase
- OPENROWSET for ad-hoc queries
- CETAS (Create External Table As Select)
- Integration with ADLS Gen2
- File format specifications (Parquet, CSV, Delta)
- No data movement - queries data in-place
- Always uses processed data source for all schemas

**Configuration**: Automatically extracted from settings object:
- `processed_external_data_source_name` - External data source for all tables
- `raw_external_data_source_name` - Available but not currently used
- `parquet_file_format` - Parquet file format name
- `csv_file_format` - CSV file format name
- Storage location prefix from `settings.full_path`

#### FabricWarehouseQueryBuilder

Optimized for Microsoft Fabric Data Warehouse:

**Features**:
- Managed Delta tables (default storage format)
- Direct OneLake integration
- V-Order optimization for Power BI
- Automatic file compaction
- Native Delta Lake ACID transactions
- Liquid clustering support

**Key Differences from Synapse**:
- Uses managed tables instead of external tables
- Simplified `USING DELTA` syntax
- Built-in Delta optimization
- No external data sources needed
- Direct OneLake paths

### QueryBuilderFactory

The factory provides convenience functions to get configured query builders:

- `get_query_builder()` - Auto-detects platform from settings and returns appropriate builder
- `get_synapse_query_builder()` - Returns Synapse builder with full type hints
- `get_fabric_query_builder()` - Returns Fabric builder with full type hints

All configuration is handled automatically from environment settings.

## Usage

**Query builders must only be accessed through factory methods.** Direct instantiation is not supported as builders require complex configuration from environment settings.

### Auto-Detection (Recommended)

Use `get_query_builder()` to automatically get the correct builder for the configured platform:

```python
from core.query_builder import get_query_builder
from core.operations import CreateTable

# Automatically detects platform (Synapse, Fabric, etc.) from settings
builder = get_query_builder()

# Generate SQL for any operation
operation = CreateTable(
    schema="silver",
    object_name="customers",
    select_query="SELECT * FROM bronze.raw_customers"
)

sql = builder.build_query(operation)
```

### Platform-Specific Builders

When you need platform-specific features or type hints, use the dedicated factory methods:

```python
# Synapse-specific builder
from core.query_builder import get_synapse_query_builder
builder = get_synapse_query_builder()

# Fabric-specific builder  
from core.query_builder import get_fabric_query_builder
builder = get_fabric_query_builder()
```

### Table Prefix Handling

Table prefixes are automatically applied based on configuration settings. The `skip_prefix_on_schema` setting controls which schemas don't receive prefixes. All identifiers are properly quoted with square brackets for safety.


## Operation Types Supported

Query builders work with operation objects from `core.operations`:

### DDL Operations
- `CreateTable`: Create tables (external or managed)
- `DropTable`: Drop tables
- `CreateSchema`: Create schemas
- `DropSchema`: Drop schemas

### DML Operations  
- `Select`: SELECT queries
- `Insert`: INSERT statements
- `Update`: UPDATE statements
- `Delete`: DELETE statements
- `Merge`: MERGE/UPSERT operations

### View Operations
- `CreateOrAlterView`: Create or alter views
- `DropView`: Drop views

### Other Operations
- `CreateStatistics`: Create table statistics
- `Copy`: COPY INTO operations
- `ExecuteSQL`: Execute arbitrary SQL

## Security Features

### Input Validation

All identifiers are validated to prevent SQL injection:

**Valid Identifiers** (will pass validation):
- `customer_id` ✓
- `order_2024` ✓
- `sales-report` ✓
- `ProductName` ✓

**Invalid Identifiers** (will be rejected):
- `drop; table users--` → Error: "Invalid identifier name"
- `123_table` → Error: "Invalid identifier name" (must start with letter)
- `user'; DELETE FROM--` → Error: "Potentially dangerous identifier"
- `a` * 130 → Error: "Identifier name too long" (max 128 chars)
- `OR 1=1` → Error: "Potentially dangerous identifier"

### Safe String Handling

String values are automatically escaped to prevent injection:

**Input String** → **Escaped Output**
- `O'Brien` → `'O''Brien'`
- `It's a test` → `'It''s a test'`
- `'; DROP TABLE--` → `'''; DROP TABLE--'`
- `Value with 'quotes'` → `'Value with ''quotes'''`

**Expression Detection** (treated as SQL, not quoted):
- `GETDATE()` → Recognized as SQL function, not quoted
- `column1 + column2` → Recognized as expression, not quoted
- `'normal string'` → Treated as string value, properly quoted

## Platform-Specific SQL Examples

### Synapse Serverless

```sql
-- External table with CETAS
CREATE EXTERNAL TABLE [silver].[sap_customers]
WITH (
    DATA_SOURCE = ProcessedDataSource,
    LOCATION = 'silver/sap_customers/',
    FILE_FORMAT = ParquetFileFormat
)
AS SELECT * FROM bronze.sap_raw_customers

-- Statistics creation
CREATE STATISTICS stat_sap_customers_customer_id 
ON [silver].[sap_customers](customer_id)
WITH FULLSCAN
```

### Fabric Warehouse

```sql
-- Managed Delta table
CREATE TABLE [silver].[sap_customers]
USING DELTA
PARTITIONED BY (year, month)
CLUSTER BY (customer_id)
AS SELECT * FROM bronze.sap_raw_customers

-- Table optimization
ANALYZE TABLE [silver].[sap_customers] COMPUTE STATISTICS
```

## Integration with Compute Module

Query builders are used internally by compute platforms:

```python
# In compute platform implementation
class SynapsePlatform:
    def __init__(self):
        self.query_builder = get_synapse_query_builder()
        self.engine = SynapseEngine(...)
    
    def create_table(self, operation: CreateTable):
        # 1. Generate SQL using query builder
        sql = self.query_builder.build_query(operation)
        
        # 2. Execute SQL using engine
        return self.engine.execute(sql)
```

## Best Practices

### DO

- ✅ Use factory methods for builder creation
- ✅ Work with typed Operation objects
- ✅ Let builders handle identifier quoting
- ✅ Trust the security validation
- ✅ Use platform-specific builders when needed

### DON'T

- ❌ Use query builders directly in application code
- ❌ Concatenate raw strings into SQL
- ❌ Bypass validation methods
- ❌ Execute queries from builders (use engines)
- ❌ Create builders manually (use factory)

## Error Handling

Common exceptions:

```python
# Invalid identifier
ValueError: "Invalid schema name: drop; table users--"

# Unsupported operation
NotImplementedError: "Operation type TRUNCATE not supported"

# Missing configuration
AttributeError: "Required setting 'data_source_name' not found"

# Platform mismatch
ValueError: "Unsupported compute type: DATABRICKS"
```

## Testing Considerations

When testing code that uses query builders:

1. **Use Factory**: Always use factory methods in tests
2. **Mock Settings**: Mock settings for consistent configuration
3. **Validate SQL**: Check generated SQL structure, not exact strings
4. **Security Tests**: Include SQL injection test cases
5. **Platform Tests**: Test platform-specific features separately

## Adding Enhancements

### Adding Support for a New Platform

To add support for a new compute platform (e.g., Databricks, Snowflake):

1. **Create a new builder class** in `query_builder/{platform}/`:
   ```python
   # query_builder/databricks/spark_builder.py
   from core.query_builder.base import BaseQueryBuilder
   from typing import TYPE_CHECKING
   
   if TYPE_CHECKING:
       from core.settings import _Settings
   
   class DatabricksSparkQueryBuilder(BaseQueryBuilder):
       def __init__(self, settings: _Settings):
           super().__init__(settings)
           # Extract any platform-specific settings if needed
           
       def _build_create_table(self, operation: CreateTable) -> str:
           # Implement Databricks-specific CREATE TABLE logic
           pass
       
       # Implement all 14 required abstract methods
   ```

2. **Update the factory** in `query_builder/factory.py`:
   ```python
   @staticmethod
   def create_databricks_builder() -> DatabricksSparkQueryBuilder:
       from core.settings import get_settings
       settings = get_settings()
       return DatabricksSparkQueryBuilder(settings)
   ```

3. **Add to the factory's create() method**:
   ```python
   if active_type == ComputeType.DATABRICKS:
       return QueryBuilderFactory.create_databricks_builder()
   ```

### Adding New Operation Types

To support new SQL operations:

1. **Define the operation** in `operations/` module
2. **Add abstract method** to `BaseQueryBuilder`:
   ```python
   @abstractmethod
   def _build_truncate(self, operation: Truncate) -> str:
       """Build TRUNCATE statement."""
       pass
   ```
3. **Implement in each platform builder**
4. **Update build_query() mapping** in base class

### Extending Platform-Specific Features

To add platform-specific methods that aren't in the base class:

```python
class SynapseServerlessQueryBuilder(BaseQueryBuilder):
    # Standard abstract methods...
    
    # Platform-specific extension
    def build_openrowset_query(self, path: str, format: str) -> str:
        """Synapse-specific OPENROWSET query."""
        return f"SELECT * FROM OPENROWSET(..."
```

### Best Practices for Enhancements

1. **Maintain Backward Compatibility**: Don't break existing interfaces
2. **Follow Layer Architecture**: Query builders stay in Layer 1
3. **Add Tests**: Every new feature needs comprehensive tests
4. **Update Documentation**: Document new platforms and operations
5. **Security First**: All new code must validate inputs
6. **Use Type Hints**: Maintain full type coverage

## Related Documentation

- **Operations Module**: [`core/operations/`](../operations/) - Operation definitions
- **Compute Module**: [`core/compute/`](../compute/) - Query execution
- **Settings Module**: [`core/settings/`](../settings/) - Configuration
- **Architecture**: [`/docs/ARCHITECTURE.md`](../../../docs/ARCHITECTURE.md) - System design

## Summary

The query_builder module is a critical infrastructure component that:

1. **Generates platform-specific SQL** from operation objects
2. **Ensures security** through comprehensive validation
3. **Abstracts platform differences** while preserving capabilities
4. **Integrates seamlessly** with compute and medallion modules
5. **Remains internal** to maintain clean API boundaries

Remember: This is an internal module. All external access to database operations should go through the public API in `core.api`.