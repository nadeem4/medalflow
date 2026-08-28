"""Provider protocol definitions.

This module defines protocols for various provider interfaces used
throughout the MedalFlow framework. These protocols ensure consistent
interfaces for different implementations.
"""

from typing import Protocol, runtime_checkable

from pydantic import SecretStr


@runtime_checkable
class SecretProvider(Protocol):
    """Protocol defining the interface for secret providers.

    All secret providers must implement this interface to ensure
    compatibility with the MedalFlow settings system.

    The protocol is marked as runtime_checkable to allow isinstance()
    checks at runtime, which is useful for validation and testing.

    ``get_secret`` is the whole contract. Caching is not part of it: the only
    cache that matters lives on :class:`~medalflow.core.descriptors.SecretField`,
    one layer above, which a provider cannot reach.
    """

    def get_secret(self, secret_name: str, default: str | None = None) -> SecretStr | None:
        """Retrieve a secret value.

        Args:
            secret_name: Name of the secret to retrieve
            default: Default value if secret not found

        Returns:
            SecretStr containing the secret value, or None if not found

        Raises:
            ValueError: If the secret cannot be retrieved and no default is provided
        """
        ...
