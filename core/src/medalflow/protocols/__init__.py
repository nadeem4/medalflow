"""Protocol definitions for MedalFlow.

This module contains protocol definitions that define contracts for
various components in the MedalFlow framework. Protocols are part of
Layer 0 and have no dependencies.

Protocols provide type-safe interfaces without requiring inheritance,
following Python's structural subtyping (duck typing with type hints).
"""

from .features import CacheProtocol, StatsProtocol
from .providers import SecretProvider

__all__ = [
    "SecretProvider",
    "CacheProtocol",
    "StatsProtocol",
]
