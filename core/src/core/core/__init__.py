"""Core utilities for MedalFlow framework.

This module provides reusable utilities, patterns, and infrastructure
components that can be used across the entire framework. As part of
Layer 1, it depends only on Layer 0 (constants and protocols).

The core module includes:
- Descriptors: Python descriptors for lazy loading, caching, etc.
- Mixins: Reusable functionality that can be mixed into classes
- Decorators: Function and class decorators for common patterns
"""

from .descriptors import SecretField, LazyField
from .mixins import SecretProviderMixin, NestedSecretsMixin

__all__ = [
    # Descriptors
    "SecretField",
    "LazyField",
    
    # Mixins
    "SecretProviderMixin",
    "NestedSecretsMixin",
]