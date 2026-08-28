"""Core logging setup and configuration.

This module wires structured JSON logging with context propagation and
OpenTelemetry correlation while keeping configuration declarative via
``logging.config.dictConfig``.
"""

from __future__ import annotations

import json
import logging
import logging.config
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

from opentelemetry import trace

from medalflow.__version__ import __version__
from medalflow.logging.filters import set_logging_context



def _build_reserved_keys() -> Set[str]:
    """Collect standard ``LogRecord`` attributes to avoid duplicating them."""
    probe = logging.LogRecord(
        name="medalflow.probe",
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg="",
        args=(),
        exc_info=None,
    )
    reserved = set(probe.__dict__.keys())
    reserved.update({"asctime", "message"})
    return reserved


_RESERVED_LOG_RECORD_KEYS = _build_reserved_keys()



def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


class CustomJsonFormatter(logging.Formatter):
    """JSON formatter that enriches log entries with context and trace data."""

    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = { }

        if hasattr(record, "resource"):
            log_record.update(dict(record.resource.attributes))

        for key, value in record.__dict__.items():
            if key not in _RESERVED_LOG_RECORD_KEYS and key not in log_record:
                log_record[key] = value

        log_record["timestamp"] = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        log_record["level"] = record.levelname
        log_record["logger"] = record.name
        log_record["message"] = record.getMessage()

        if hasattr(record, "otelTraceID"):
            log_record["trace_id"] = record.otelTraceID

        if hasattr(record, "otelSpanID"):
            log_record["span_id"] = record.otelSpanID

        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)


        return json.dumps(log_record, default=str)


def setup_logging(
    level: str = "INFO",
    *,
    service_name: str = "medalflow",
    service_version: Optional[str] = None,
    environment: Optional[str] = None,
    static_fields: Optional[Dict[str, Any]] = None,
) -> None:
    """Configure structured logging backed by ``logging.config.dictConfig``.

    Args:
        level: Base log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        service_name: Logical service name for log enrichment.
        service_version: Service version override (defaults to package version).
        environment: Deployment environment label (dev/staging/prod).
        static_fields: Additional static fields to attach to all log records.
    """
    set_logging_context(
        environment=environment,
        service_name=service_name,
        service_version=service_version or __version__,
        extra=static_fields,
    )
    config_dict: Dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "cte_json": {
                "()": "medalflow.logging.logger.CustomJsonFormatter",
            }
        },
        "filters": {
            "cte_context": {
                "()": "medalflow.logging.filters.ContextFilter",
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": level.upper(),
                "formatter": "cte_json",
                "filters": ["cte_context"],
                "stream": "ext://sys.stdout",
            }
        },
        "root": {
            "level": level.upper(),
            "handlers": ["console"],
        },
    }

    logging.config.dictConfig(config_dict)
