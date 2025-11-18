"""Common utilities and exceptions for MedalFlow.

This module provides common functionality used throughout the MedalFlow package,
including custom exceptions, validation framework, and orchestration utilities.

Key Components:
    - **Exceptions**: Comprehensive exception hierarchy with error codes
    - **Orchestration**: Parameter enrichment for medallion layer processing
    - **Validation**: Factory-based validation framework for data quality

Exception Design:
    The exception system uses error codes for categorization rather than
    numerous specific exception classes. All exceptions inherit from CTEError
    and include structured error information.

Orchestration:
    Parameter providers enrich function parameters with standardized
    configuration for each medallion layer (Bronze, Silver, Gold).

Validation:
    Factory-based validation system allows pluggable validators for
    different data quality checks.
"""

from core.common.exceptions import (
    CTEError,
    ErrorCode,
    # Helper functions
    configuration_error,
    validation_error,
    connection_error,
    execution_error,
    query_execution_error,
    compute_error,
    job_submission_error,
    job_status_error,
    platform_not_supported_error,
    retryable_error,
    feature_not_enabled_error,
    resource_not_found_error,
)

from core.utils.decorators import retry_with_backoff

__all__ = [
    # Base Exception and Error Codes
    "CTEError",
    "ErrorCode",
    # Helper functions
    "configuration_error",
    "validation_error",
    "connection_error",
    "execution_error",
    "query_execution_error",
    "compute_error",
    "job_submission_error",
    "job_status_error",
    "platform_not_supported_error",
    "retryable_error",
    "feature_not_enabled_error",
    "resource_not_found_error",
    # Utilities
    "retry_with_backoff",
]