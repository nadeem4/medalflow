# MedalFlow Logging Module

## Overview

The `core.logging` package supplies a structured logging foundation for the MedalFlow ETL framework. It emits JSON logs, enriches them with request and trace metadata, and stays sink-agnostic so host applications decide where records are delivered.

## Key features

- **Structured JSON** payloads with timestamp, service metadata, and OpenTelemetry trace correlation
- **Context propagation** backed by `ContextFilter` and async-safe context variables
- **Declarative configuration** via `logging.config.dictConfig` with sensible defaults
- **Domain adapters** that append engine, table, or sequencer context automatically

## Layout

- `logger.py` – `setup_logging`, `get_logger`, and `CustomJsonFormatter`
- `filters.py` – request-scoped context variables, utilities, and `ContextFilter`
- `__init__.py` – public exports

## Usage

### Basic logging

```python
from core.logging import get_logger

logger = get_logger(__name__)
logger.info("Processing started", table="customers", rows=1000)
logger.error("Processing failed", error_code="E001", details="Connection timeout")
```

### Configuring logging

`setup_logging` provisions a standard JSON console handler; all knobs are exposed through explicit parameters:

```python
from core.logging import setup_logging

setup_logging(
    level="INFO",
    service_name="medalflow",
    service_version="2.0.0",
    environment="production",
    static_fields={"deployment": "azure-functions"},
)
```

Additional handlers can be added by the host application after calling `setup_logging` using the standard Python logging APIs if absolutely required, but the default configuration should cover most scenarios.

### Context variables

```python
from core.logging import get_logger
from core.logging.filters import , set_request_context

logger = get_logger(__name__)


# Or set fields directly for the current context
set_request_context(request_id="req-456", user_id="user-001")
logger.info("User action performed")
```

## Log output example

```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "level": "INFO",
  "logger": "core.compute.engines.synapse",
  "message": "Query executed successfully",
  "service": "medalflow",
  "service_version": "2.0.0",
  "environment": "production",
  "request_id": "req-123",
  "operation": "create_silver_tables",
  "engine_type": "synapse_sql",
  "table": "customers",
  "schema": "silver",
  "rows": 1000,
  "duration_ms": 250
}
```

## Tips

- Use `get_logger(__name__)` for module-level loggers
- Enrich logs with keyword arguments instead of string interpolation
- Set request context at the beginning of request/execution scopes
- If you need additional sinks, attach handlers to the root logger after invoking `setup_logging`
- Avoid logging secrets; include measurable values (counts, durations, sizes)

## Troubleshooting

- Nothing logged? Check log level and ensure `setup_logging` runs before first log.
- Missing context? Verify `set_request_context` usage.
- Too chatty? Adjust handler or root log levels inside your configuration.

## Future enhancements

- Recipes for common sink configurations in host repositories
- Sample integrations demonstrating OpenTelemetry log exporters
- Log sampling utilities for high-volume workloads
