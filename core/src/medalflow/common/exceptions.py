from enum import Enum
from typing import Any, Optional


class ErrorCode(Enum):
    """Standard error codes for medalflow operations.

    This enum provides categorized error codes that can be used
    to identify error types without creating numerous exception classes.
    Each category has a specific number range for easy identification.

    Attributes:
        CONFIG_*: Configuration-related errors (1xxx)
        CONNECTION_*: Network and connection errors (3xxx)
        EXECUTION_*: Runtime execution errors (4xxx)
        OPERATION_*: High-level operation errors (8xxx)
    """

    # Configuration errors (1xxx)
    FEATURE_DISABLED = "CONFIG_004"

    # Connection errors (3xxx)
    CONNECTION_ERROR = "CONNECTION_001"

    # Execution errors (4xxx)
    QUERY_EXECUTION_ERROR = "EXECUTION_002"

    # Operation errors (8xxx)
    OPERATION_ERROR = "OPERATION_001"


class CTEError(Exception):
    """Base exception for all medalflow-related errors.

    This simplified exception class uses error codes for categorization
    instead of creating numerous specific exception classes.

    Attributes:
        message: Error message
        error_code: Error code from ErrorCode enum
        details: Additional error details
        cause: Optional underlying exception
        is_retryable: Whether the error is transient and can be retried
    """

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.OPERATION_ERROR,
        details: Optional[dict[str, Any]] = None,
        cause: Optional[Exception] = None,
        is_retryable: bool = False,
    ):
        """Initialize medalflow error.

        Args:
            message: Error message
            error_code: Error code from ErrorCode enum
            details: Additional error details
            cause: Optional underlying exception
            is_retryable: Whether error is transient
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.cause = cause
        self.is_retryable = is_retryable

        # Use structured logger (lazy import to avoid circular dependency)
        from medalflow.logging import get_logger

        logger = get_logger(__name__)
        logger.error(
            message,
            extra={
                "error_code": error_code.value,
                "details": self.details,
                "is_retryable": is_retryable,
            },
            exc_info=cause is not None,
        )

    def __str__(self) -> str:
        """String representation of the error."""
        msg = f"[{self.error_code.value}] {self.message}"
        if self.cause:
            msg = f"{msg} (caused by: {type(self.cause).__name__}: {str(self.cause)})"
        return msg


# Helper functions for common error scenarios
def connection_error(
    message: str, service: Optional[str] = None, host: Optional[str] = None, **kwargs
) -> CTEError:
    """Create a connection error.

    Args:
        message: Error message
        service: Service that failed to connect
        host: Host/endpoint that failed
        **kwargs: Additional error details

    Returns:
        CTEError with CONNECTION_ERROR code
    """
    details = kwargs.get("details", {})
    if service:
        details["service"] = service
    if host:
        details["host"] = host

    return CTEError(
        message=message,
        error_code=ErrorCode.CONNECTION_ERROR,
        details=details,
        **{k: v for k, v in kwargs.items() if k != "details"},
    )


def query_execution_error(query: str, original_error: Exception, **kwargs) -> CTEError:
    """Create a query execution error.

    Args:
        query: SQL query that failed
        original_error: The underlying exception
        **kwargs: Additional error details

    Returns:
        CTEError with QUERY_EXECUTION_ERROR code
    """
    details = kwargs.get("details", {})
    details["query"] = query[:500] + "..." if len(query) > 500 else query

    return CTEError(
        message=f"Query execution failed: {str(original_error)}",
        error_code=ErrorCode.QUERY_EXECUTION_ERROR,
        details=details,
        cause=original_error,
        **{k: v for k, v in kwargs.items() if k not in ["details", "cause"]},
    )


def feature_not_enabled_error(feature_name: str, message: str = "", **kwargs) -> CTEError:
    """Create a feature not enabled error.

    Args:
        feature_name: Name of the feature that is disabled
        message: Additional message/guidance
        **kwargs: Additional error details

    Returns:
        CTEError with FEATURE_DISABLED code
    """
    full_message = (
        f"{feature_name} is not enabled. "
        f"Please reach out to CMAA team to enable this feature. {message}"
    ).strip()

    details = kwargs.get("details", {})
    details["config_key"] = f"feature.{feature_name}"
    details["feature"] = feature_name

    return CTEError(
        message=full_message,
        error_code=ErrorCode.FEATURE_DISABLED,
        details=details,
        **{k: v for k, v in kwargs.items() if k != "details"},
    )
