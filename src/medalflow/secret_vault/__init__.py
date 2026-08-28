"""Secret vault package for managing secrets from various providers.

This package provides implementations of the SecretProvider protocol,
allowing different secret providers (KeyVault, Mock, etc.) to be used
interchangeably through a common interface.

The package follows the Protocol pattern to ensure loose coupling and
easy testing, while maintaining type safety through Python's typing system.

This is the **secrets seam**, one of MedalFlow's three integration points
(compute, storage, secrets -- see :mod:`medalflow.protocols`). Implementing
``get_secret`` is the whole contract:

- :class:`~medalflow.secret_vault.env.EnvSecretProvider` -- the default, reads
  ``MEDALFLOW_SECRET_<NAME>`` and needs no cloud SDK.
- :class:`~medalflow.secret_vault.keyvault.KeyVaultSecrets` -- Azure Key Vault.
- :class:`~medalflow.secret_vault.mock.MockSecrets` -- test mode.
"""

from medalflow.protocols.providers import SecretProvider

from .env import EnvSecretProvider

__all__ = [
    "SecretProvider",
    "EnvSecretProvider",
]
