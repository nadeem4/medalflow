"""Decorators for MedalFlow.

This module contains reusable decorators for common patterns.
Currently provides the feature_gate decorator for FeatureManager classes.

Future decorators may include:
- @retry: Retry failed operations
- @cache: Cache function results
- @validate: Validate function arguments
- @timed: Measure execution time
- @logged: Log function calls
"""

from .features import feature_gate

__all__ = ['feature_gate']