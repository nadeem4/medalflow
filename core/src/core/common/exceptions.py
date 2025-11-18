from enum import Enum
from typing import Any, Dict, Optional


class ErrorCode(Enum):
    """Standard error codes for medalflow operations.
    
    This enum provides categorized error codes that can be used
    to identify error types without creating numerous exception classes.
    Each category has a specific number range for easy identification.
    
    Attributes:
        CONFIG_*: Configuration-related errors (1xxx)
        VALIDATION_*: Input validation errors (2xxx)
        CONNECTION_*: Network and connection errors (3xxx)
        EXECUTION_*: Runtime execution errors (4xxx)
        RESOURCE_*: Resource availability errors (5xxx)
        DATA_*: Data quality and integrity errors (6xxx)
        PLATFORM_*: Platform-specific errors (7xxx)
        OPERATION_*: High-level operation errors (8xxx)
        RETRY_*: Transient/retryable errors (9xxx)
    """
    # Configuration errors (1xxx)
    CONFIG_ERROR = "CONFIG_001"
    CONFIG_MISSING = "CONFIG_002"
    CONFIG_INVALID = "CONFIG_003"
    FEATURE_DISABLED = "CONFIG_004"
    
    # Validation errors (2xxx)
    VALIDATION_ERROR = "VALIDATION_001"
    INVALID_ARGUMENT = "VALIDATION_002"
    MISSING_PARAMETER = "VALIDATION_003"
    INVALID_IDENTIFIER = "VALIDATION_004"
    
    # Connection errors (3xxx)
    CONNECTION_ERROR = "CONNECTION_001"
    AUTH_ERROR = "CONNECTION_002"
    TIMEOUT_ERROR = "CONNECTION_003"
    
    # Execution errors (4xxx)
    EXECUTION_ERROR = "EXECUTION_001"
    QUERY_EXECUTION_ERROR = "EXECUTION_002"
    JOB_SUBMISSION_ERROR = "EXECUTION_003"
    JOB_STATUS_ERROR = "EXECUTION_004"
    TRANSFORMATION_ERROR = "EXECUTION_005"
    
    # Resource errors (5xxx)
    RESOURCE_NOT_FOUND = "RESOURCE_001"
    TABLE_NOT_FOUND = "RESOURCE_002"
    FILE_NOT_FOUND = "RESOURCE_003"
    SECRET_NOT_FOUND = "RESOURCE_004"
    
    # Data errors (6xxx)
    DATA_QUALITY_ERROR = "DATA_001"
    DUPLICATE_KEY_ERROR = "DATA_002"
    DATA_INTEGRITY_ERROR = "DATA_003"
    
    # Platform errors (7xxx)
    PLATFORM_ERROR = "PLATFORM_001"
    PLATFORM_NOT_SUPPORTED = "PLATFORM_002"
    ENGINE_NOT_AVAILABLE = "PLATFORM_003"
    
    # Operation errors (8xxx)
    OPERATION_ERROR = "OPERATION_001"
    LAYER_PROCESSING_ERROR = "OPERATION_002"
    COPY_OPERATION_ERROR = "OPERATION_003"
    TABLE_OPERATION_ERROR = "OPERATION_004"
    ADLS_OPERATION_ERROR = "OPERATION_005"
    
    # Retry/Transient errors (9xxx)
    RETRYABLE_ERROR = "RETRY_001"
    RATE_LIMIT_ERROR = "RETRY_002"


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
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
        is_retryable: bool = False
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
        from core.logging import get_logger
        logger = get_logger(__name__)
        logger.error(
            message,
            error_code=error_code.value,
            details=details,
            is_retryable=is_retryable,
            exc_info=cause is not None
        )
        
    def __str__(self) -> str:
        """String representation of the error."""
        msg = f"[{self.error_code.value}] {self.message}"
        if self.cause:
            msg = f"{msg} (caused by: {type(self.cause).__name__}: {str(self.cause)})"
        return msg
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for serialization."""
        return {
            "type": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code.value,
            "error_name": self.error_code.name,
            "details": self.details,
            "is_retryable": self.is_retryable
        }
    
    @classmethod
    def from_error_code(
        cls,
        error_code: ErrorCode,
        message: str,
        **kwargs
    ) -> "CTEError":
        """Create exception from error code.
        
        Args:
            error_code: Error code
            message: Error message
            **kwargs: Additional arguments for CTEError
            
        Returns:
            CTEError instance
        """
        # Mark certain errors as retryable by default
        # These error types are typically transient and can succeed on retry
        if error_code in [
            ErrorCode.TIMEOUT_ERROR,
            ErrorCode.RETRYABLE_ERROR,
            ErrorCode.RATE_LIMIT_ERROR
        ]:
            # Only set is_retryable if not explicitly provided by caller
            kwargs.setdefault('is_retryable', True)
            
        return cls(message=message, error_code=error_code, **kwargs)


# Helper functions for common error scenarios
def configuration_error(
    message: str,
    config_key: Optional[str] = None,
    **kwargs
) -> CTEError:
    """Create a configuration error.
    
    Args:
        message: Error message
        config_key: Configuration key that caused the error
        **kwargs: Additional error details
        
    Returns:
        CTEError with CONFIG_ERROR code
    """
    details = kwargs.get('details', {})
    if config_key:
        details["config_key"] = config_key
    
    return CTEError(
        message=message,
        error_code=ErrorCode.CONFIG_ERROR,
        details=details,
        **{k: v for k, v in kwargs.items() if k != 'details'}
    )


def validation_error(
    message: str,
    field: Optional[str] = None,
    value: Any = None,
    **kwargs
) -> CTEError:
    """Create a validation error.
    
    Args:
        message: Error message
        field: Field that failed validation
        value: Invalid value
        **kwargs: Additional error details
        
    Returns:
        CTEError with VALIDATION_ERROR code
    """
    details = kwargs.get('details', {})
    if field:
        details["field"] = field
    if value is not None:
        details["value"] = str(value)
    
    return CTEError(
        message=message,
        error_code=ErrorCode.VALIDATION_ERROR,
        details=details,
        **{k: v for k, v in kwargs.items() if k != 'details'}
    )


def connection_error(
    message: str,
    service: Optional[str] = None,
    host: Optional[str] = None,
    **kwargs
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
    details = kwargs.get('details', {})
    if service:
        details["service"] = service
    if host:
        details["host"] = host
    
    return CTEError(
        message=message,
        error_code=ErrorCode.CONNECTION_ERROR,
        details=details,
        **{k: v for k, v in kwargs.items() if k != 'details'}
    )


def execution_error(
    message: str,
    operation: Optional[str] = None,
    query: Optional[str] = None,
    **kwargs
) -> CTEError:
    """Create an execution error.
    
    Args:
        message: Error message
        operation: Operation that failed
        query: Query that failed (if applicable)
        **kwargs: Additional error details
        
    Returns:
        CTEError with EXECUTION_ERROR code
    """
    details = kwargs.get('details', {})
    if operation:
        details["operation"] = operation
    if query:
        # Truncate long queries to prevent log bloat and potential security issues
        # Keep first 500 chars which usually contains the important parts (SELECT, CREATE, etc.)
        details["query"] = query[:500] + "..." if len(query) > 500 else query
    
    return CTEError(
        message=message,
        error_code=ErrorCode.EXECUTION_ERROR,
        details=details,
        **{k: v for k, v in kwargs.items() if k != 'details'}
    )


def query_execution_error(
    query: str,
    original_error: Exception,
    **kwargs
) -> CTEError:
    """Create a query execution error.
    
    Args:
        query: SQL query that failed
        original_error: The underlying exception
        **kwargs: Additional error details
        
    Returns:
        CTEError with QUERY_EXECUTION_ERROR code
    """
    details = kwargs.get('details', {})
    details["query"] = query[:500] + "..." if len(query) > 500 else query
    
    return CTEError(
        message=f"Query execution failed: {str(original_error)}",
        error_code=ErrorCode.QUERY_EXECUTION_ERROR,
        details=details,
        cause=original_error,
        **{k: v for k, v in kwargs.items() if k not in ['details', 'cause']}
    )


def compute_error(
    message: str,
    operation: Optional[str] = None,
    **kwargs
) -> CTEError:
    """Create a compute error.
    
    Args:
        message: Error message
        operation: Operation that failed
        **kwargs: Additional error details
        
    Returns:
        CTEError with EXECUTION_ERROR code
    """
    details = kwargs.get('details', {})
    if operation:
        details["operation"] = operation
    
    return CTEError(
        message=message,
        error_code=ErrorCode.EXECUTION_ERROR,
        details=details,
        **{k: v for k, v in kwargs.items() if k != 'details'}
    )


def job_submission_error(
    job_spec: str,
    original_error: Exception,
    **kwargs
) -> CTEError:
    """Create a job submission error.
    
    Args:
        job_spec: Job specification that failed to submit
        original_error: The underlying exception
        **kwargs: Additional error details
        
    Returns:
        CTEError with JOB_SUBMISSION_ERROR code
    """
    details = kwargs.get('details', {})
    details["job_spec"] = job_spec
    
    return CTEError(
        message=f"Failed to submit job: {str(original_error)}",
        error_code=ErrorCode.JOB_SUBMISSION_ERROR,
        details=details,
        cause=original_error,
        **{k: v for k, v in kwargs.items() if k not in ['details', 'cause']}
    )


def job_status_error(
    job_id: str,
    original_error: Exception,
    **kwargs
) -> CTEError:
    """Create a job status error.
    
    Args:
        job_id: Job ID that failed status check
        original_error: The underlying exception
        **kwargs: Additional error details
        
    Returns:
        CTEError with JOB_STATUS_ERROR code
    """
    details = kwargs.get('details', {})
    details["job_id"] = job_id
    
    return CTEError(
        message=f"Failed to get status for job {job_id}: {str(original_error)}",
        error_code=ErrorCode.JOB_STATUS_ERROR,
        details=details,
        cause=original_error,
        **{k: v for k, v in kwargs.items() if k not in ['details', 'cause']}
    )


def platform_not_supported_error(
    platform: str,
    **kwargs
) -> CTEError:
    """Create a platform not supported error.
    
    Args:
        platform: Platform that is not supported
        **kwargs: Additional error details
        
    Returns:
        CTEError with PLATFORM_NOT_SUPPORTED code
    """
    details = kwargs.get('details', {})
    details["platform"] = platform
    
    return CTEError(
        message=f"Platform '{platform}' is not supported",
        error_code=ErrorCode.PLATFORM_NOT_SUPPORTED,
        details=details,
        **{k: v for k, v in kwargs.items() if k != 'details'}
    )


def retryable_error(
    message: str,
    retry_after: Optional[int] = None,
    attempt: Optional[int] = None,
    **kwargs
) -> CTEError:
    """Create a retryable error.
    
    Args:
        message: Error message
        retry_after: Seconds to wait before retry
        attempt: Current attempt number
        **kwargs: Additional error details
        
    Returns:
        CTEError with RETRYABLE_ERROR code and is_retryable=True
    """
    details = kwargs.get('details', {})
    if retry_after:
        details["retry_after"] = retry_after
    if attempt:
        details["attempt"] = attempt
    
    return CTEError(
        message=message,
        error_code=ErrorCode.RETRYABLE_ERROR,
        details=details,
        is_retryable=True,
        **{k: v for k, v in kwargs.items() if k != 'details'}
    )


def feature_not_enabled_error(
    feature_name: str,
    message: str = "",
    **kwargs
) -> CTEError:
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
    
    details = kwargs.get('details', {})
    details["config_key"] = f"feature.{feature_name}"
    details["feature"] = feature_name
    
    return CTEError(
        message=full_message,
        error_code=ErrorCode.FEATURE_DISABLED,
        details=details,
        **{k: v for k, v in kwargs.items() if k != 'details'}
    )


def resource_not_found_error(
    message: str,
    resource_type: Optional[str] = None,
    resource_name: Optional[str] = None,
    **kwargs
) -> CTEError:
    """Create a resource not found error.
    
    Args:
        message: Error message
        resource_type: Type of resource (table, file, etc.)
        resource_name: Name of the missing resource
        **kwargs: Additional error details
        
    Returns:
        CTEError with RESOURCE_NOT_FOUND code
    """
    details = kwargs.get('details', {})
    if resource_type:
        details["resource_type"] = resource_type
    if resource_name:
        details["resource_name"] = resource_name
    
    return CTEError(
        message=message,
        error_code=ErrorCode.RESOURCE_NOT_FOUND,
        details=details,
        **{k: v for k, v in kwargs.items() if k != 'details'}
    )


















































