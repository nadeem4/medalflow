"""Python descriptors for MedalFlow.

This module provides Python descriptors that implement common patterns
like lazy loading, caching, and validation at the attribute level.
"""

from .secrets import SecretField
from .lazy import LazyField

__all__ = [
    "SecretField",
    "LazyField",
]