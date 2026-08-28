"""Logging filters for context injection.

This module provides filters that inject context variables into log records,
enabling correlation of logs across requests and operations.
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import Any

from medalflow.__version__ import __version__

request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)
user_id_var: ContextVar[str | None] = ContextVar("user_id", default=None)

_service_name: str | None = "medalflow"
_service_version: str | None = __version__
_environment: str | None = None
_static_fields: dict[str, Any] = {}


class ContextFilter(logging.Filter):
    """Logging filter that adds context variables to log records.

    This filter extracts values from context variables and adds them to
    log records, enabling log correlation across async operations.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Add context variables to the log record.

        Args:
            record: Log record to enhance

        Returns:
            Always True (doesn't filter out any records)
        """
        record.request_id = request_id_var.get()
        record.user_id = user_id_var.get()
        record.sdk_name = "medalflow"
        record.core_version = __version__

        if _service_name:
            record.service = _service_name
        if _service_version:
            record.service_version = _service_version
        if _environment:
            record.environment = _environment

        for key, value in _static_fields.items():
            if not hasattr(record, key):
                setattr(record, key, value)

        return True


def set_logging_context(
    *,
    environment: str | None = None,
    service_name: str | None = None,
    service_version: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    """Set static logging context fields for all log records."""
    global _service_name, _service_version, _environment, _static_fields

    if service_name is not None:
        _service_name = service_name
    if service_version is not None:
        _service_version = service_version

    if environment:
        _environment = environment
    else:
        _environment = None

    if extra is None:
        _static_fields = {}
    else:
        _static_fields = dict(extra)


def set_request_context(
    request_id: str | None = None,
    user_id: str | None = None,
) -> None:
    """Set request context variables."""
    if request_id is not None:
        request_id_var.set(request_id)
    if user_id is not None:
        user_id_var.set(user_id)


def clear_request_context() -> None:
    """Clear all request context variables."""
    request_id_var.set(None)
    user_id_var.set(None)
