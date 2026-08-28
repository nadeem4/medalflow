"""Protocol definitions for MedalFlow.

This module contains protocol definitions that define contracts for
various components in the MedalFlow framework. Protocols are part of
Layer 0 and have no dependencies.

Protocols provide type-safe interfaces without requiring inheritance,
following Python's structural subtyping (duck typing with type hints).

The three integration seams
---------------------------
MedalFlow reaches outside itself in exactly three places, and each one is a
protocol you can implement to run the framework somewhere else:

- **compute** -- ``medalflow.compute.platforms.base._BasePlatform``, built by
  ``medalflow.compute.factory.create_platform``. Executes operations against a
  SQL platform; ``SynapsePlatform`` is the implementation that ships.
- **storage** -- :class:`~medalflow.protocols.storage.StorageClient`. Deletes a
  path and reads a CSV; ``DatalakeClient`` (ADLS Gen2) is the implementation
  that ships.
- **secrets** -- :class:`~medalflow.protocols.providers.SecretProvider`. Reads
  one named secret. ``KeyVaultSecrets``, ``EnvSecretProvider`` (the
  zero-dependency default) and ``MockSecrets`` ship.

These three are the whole integration surface. Nothing else in the package is
an extension point, and no abstraction is offered for a cloud provider that
has no implementation here.
"""

from .features import CacheProtocol, StatsProtocol
from .providers import SecretProvider
from .storage import StorageClient

__all__ = [
    "SecretProvider",
    "StorageClient",
    "CacheProtocol",
    "StatsProtocol",
]
