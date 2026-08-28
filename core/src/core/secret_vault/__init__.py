"""Secret vault package for managing secrets from various providers.

This package provides implementations of the SecretProvider protocol,
allowing different secret providers (KeyVault, Mock, etc.) to be used
interchangeably through a common interface.

The package follows the Protocol pattern to ensure loose coupling and
easy testing, while maintaining type safety through Python's typing system.
"""

from core.protocols.providers import SecretProvider

__all__ = [
    "SecretProvider",
]