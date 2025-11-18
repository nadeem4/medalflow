"""Protocol definitions for MedalFlow.

This module contains protocol definitions that define contracts for
various components in the MedalFlow framework. Protocols are part of
Layer 0 and have no dependencies.

Protocols provide type-safe interfaces without requiring inheritance,
following Python's structural subtyping (duck typing with type hints).
"""

from .providers import SecretProvider, ConfigProvider, ConfigurationProvider
from .features import CacheProtocol, ClientConfigProtocol, SilverGroupingProtocol, StatsProtocol, PowerBIProtocol

__all__ = [
    "SecretProvider",
    "ConfigProvider",
    "ConfigurationProvider",
    "CacheProtocol",
    "ClientConfigProtocol",
    "SilverGroupingProtocol",
    "StatsProtocol",
    "PowerBIProtocol",
]