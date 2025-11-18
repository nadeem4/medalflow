# Utils Module

This module provides essential utility functions and decorators used throughout the medalflow package. It offers cross-cutting functionality for error handling, performance monitoring, datetime operations, and retry logic.

## Overview

The utils module contains three main components:

- **datetime.py**: Date and time utilities for snapshot management and partition paths
- **decorators.py**: Function decorators for retry logic, timing, error handling, and synchronization
- **validators** (planned): Input validation utilities for SQL identifiers and data validation

## Module Structure

```
utils/
├── __init__.py        # Module exports and public API
├── datetime.py        # DateTime and partition path utilities  
├── decorators.py      # Decorator utilities
└── README.md         # This file
```

## DateTime Utilities

### Core Functions

#### `get_current_timestamp() -> datetime`
Returns the current UTC timestamp. Used throughout the system for consistent timestamp generation.

#### `get_snapshot_datetime() -> str`
Returns a formatted datetime string in 'YYYY-MM-DD HH:MM:SS' format for snapshot operations.

#### `get_partition_path(base_path, frequency, timestamp=None) -> str`
Generates hierarchical partition paths based on snapshot frequency:
- Daily: `base_path/daily/2024/01/15`
- Hourly: `base_path/hourly/2024/01/15/14`
- Weekly: `base_path/weekly/2024/week_03`
- Monthly: `base_path/monthly/2024/01`
- Quarterly: `base_path/quarterly/2024/q1`
- Yearly: `base_path/yearly/2024`

#### `parse_snapshot_path(path) -> Dict[str, str]`
Extracts date components from partition paths for analysis and querying.

#### `get_date_range_for_frequency(frequency, reference_date=None) -> Tuple[datetime, datetime]`
Calculates start and end dates for a given frequency period.

### Usage Example

```python
from core.utils import get_partition_path, parse_snapshot_path
from core.config.constants import SnapshotFrequency

# Generate partition path
path = get_partition_path(
    "silver/inventory",
    SnapshotFrequency.DAILY,
    datetime(2024, 1, 15)
)
# Result: "silver/inventory/daily/2024/01/15"

# Parse existing path
components = parse_snapshot_path(path)
# Result: {'frequency': 'daily', 'year': '2024', 'month': '01', 'day': '15'}
```

## Decorator Utilities

### Retry Decorators

#### `@retry_with_backoff(...)`
Retries operations with exponential backoff. Works with both sync and async functions.

**Parameters:**
- `max_retries`: Maximum retry attempts (default: 3)
- `initial_delay`: Initial delay in seconds (default: 1.0)
- `max_delay`: Maximum delay cap (default: 60.0)
- `exponential_base`: Backoff multiplier (default: 2.0)
- `retry_on`: Tuple of exception types to retry
- `retry_condition`: Custom function to determine retry

**Example:**
```python
from core.utils import retry_with_backoff

@retry_with_backoff(
    max_retries=5,
    retry_on=(ConnectionError, TimeoutError),
    initial_delay=2.0
)
async def fetch_data():
    return await api_call()
```

### Timeout Decorator

#### `@with_timeout(timeout_seconds, timeout_exception=None)`
Adds timeout to async functions to prevent indefinite blocking.

**Example:**
```python
from core.utils import with_timeout

@with_timeout(30.0)
async def fetch_large_dataset():
    return await download_data()
```

### Error Handling Decorator

#### `@catch_exception(...)`
Gracefully handles exceptions with optional transformation.

**Parameters:**
- `exception_type`: Exception type to catch
- `default_return`: Value to return on exception
- `log_error`: Whether to log the error
- `raise_new`: Optional exception type to raise instead

**Example:**
```python
from core.utils import catch_exception

@catch_exception(ValueError, default_return=0)
def parse_number(s: str) -> int:
    return int(s)
```

### Performance Monitoring

**Parameters:**
- `log_start`: Log function start (default: True)
- `log_args`: Include arguments in logs (default: False)
- `metric_name`: Optional metric name for monitoring


### Deprecation Decorator

#### `@deprecated(reason, version=None, alternative=None)`
Marks functions as deprecated with migration guidance.

**Example:**
```python
from core.utils import deprecated

@deprecated(
    reason="Use process_data_streaming for better performance",
    version="2.0.0",
    alternative="process_data_streaming"
)
def process_data_batch(data: list) -> dict:
    return process_all_at_once(data)
```

### Synchronization Decorator

#### `@synchronized(lock=None)`
Ensures only one instance of an async function runs at a time.

**Example:**
```python
from core.utils import synchronized

@synchronized()
async def update_shared_resource(data: dict):
    current = await read_resource()
    current.update(data)
    await write_resource(current)
```

## Best Practices

### 1. Retry Logic
- Use specific exception types in `retry_on` parameter
- Set reasonable `max_delay` to prevent excessive waiting
- Consider using `retry_condition` for content-based retry decisions

### 2. Error Handling
- Prefer specific exception types over catching `Exception`
- Always log errors unless explicitly not needed
- Use `raise_new` to transform internal exceptions at API boundaries

### 3. Performance Monitoring
- Use `metric_name` for production monitoring integration
- Be cautious with `log_args` when dealing with sensitive data
- Consider decorator order when combining multiple decorators

### 4. Datetime Operations
- Always use UTC timestamps for consistency
- Use `get_partition_path` for standardized data lake paths
- Leverage `parse_snapshot_path` for partition analysis

## Integration with MedalFlow

The utils module integrates seamlessly with other medalflow components:

- **Compute Layer**: Decorators are used for retry logic in SQL operations
- **Data Lake**: DateTime utilities manage partition paths
- **Settings**: Some utilities access configuration via `get_settings()`
- **Logging**: All decorators use the structured logging framework

## Future Enhancements

### Planned Validators Module
The validators module will provide:
- SQL identifier validation
- Schema and table name validation  
- Column name validation
- Data type validation utilities


## See Also

- [MedalFlow Documentation](../../README.md)
- [Settings Module](../settings/README.md)
- [Logging Module](../logging/README.md)
- [Common Module](../common/README.md)