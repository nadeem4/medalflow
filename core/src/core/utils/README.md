# Utils Module

This module provides essential utility functions and decorators used throughout the medalflow package. It offers cross-cutting functionality for tracing, datetime operations, and retry logic.

## Overview

The utils module contains three main components:

- **datetime.py**: Date and time utilities for snapshot management and partition paths
- **decorators.py**: Function decorators for retry logic and OpenTelemetry tracing
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

## Best Practices

### 1. Retry Logic
- Use specific exception types in `retry_on` parameter
- Set reasonable `max_delay` to prevent excessive waiting
- Consider using `retry_condition` for content-based retry decisions

### 2. Datetime Operations
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