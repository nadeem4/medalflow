"""Validation constants and enumerations.

This module contains enum types used for data validation
and quality checks.
"""

from enum import Enum


class ValidationLevel(str, Enum):
    """Validation level enumeration.
    
    Defines the severity levels for data validation rules.
    
    Values:
        ERROR: Critical validation that must pass
        WARNING: Non-critical validation that generates warnings
        INFO: Informational validation for monitoring
    """
    
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"