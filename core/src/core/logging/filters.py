"""Logging filters for context injection.

This module provides filters that inject context variables into log records,
enabling correlation of logs across requests and operations.
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import Any, Dict, Optional
from core.__version__ import __version__

request_id_var: ContextVar[Optional[str]] = ContextVar("request_id", default=None)
user_id_var: ContextVar[Optional[str]] = ContextVar("user_id", default=None)

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
        setattr(record, "request_id", request_id_var.get())
        setattr(record, "user_id", user_id_var.get())
        setattr(record, "sdk_name", "medalflow")
        setattr(record, "core_version", __version__)


        return True


def set_request_context(
    request_id: Optional[str] = None,
    user_id: Optional[str] = None,
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