"""Azure KeyVault secret provider implementation.

This module provides the KeyVaultSecrets class which implements the
SecretProvider protocol for retrieving secrets from Azure Key Vault.
"""

import random
import time
from typing import TYPE_CHECKING, Optional

from pydantic import SecretStr

from medalflow.common.exceptions import CTEError, ErrorCode
from medalflow.logging import get_logger

if TYPE_CHECKING:
    from azure.keyvault.secrets import SecretClient

    from medalflow.settings.keyvault import KeyVaultSettings

logger = get_logger(__name__)

# Azure failures that will never succeed on a second attempt. Matched by class
# name so this module keeps its azure imports function-local.
_NON_RETRYABLE_ERRORS = frozenset(
    {
        "ClientAuthenticationError",
        "ResourceExistsError",
        "ResourceNotFoundError",
        "ServiceRequestTimeoutError",
    }
)


def _is_retryable(error: Exception) -> bool:
    """Decide whether a Key Vault failure is worth another attempt.

    Args:
        error: The exception raised by the Azure SDK

    Returns:
        True for transient transport and throttling failures; False for a
        missing SDK, a missing secret, or rejected credentials.
    """
    if isinstance(error, ImportError | CTEError):
        return False

    if type(error).__name__ in _NON_RETRYABLE_ERRORS:
        return False

    status_code = getattr(error, "status_code", None)
    if status_code is not None:
        return status_code == 429 or status_code >= 500

    return True


class KeyVaultSecrets:
    """Azure Key Vault secret provider implementation.

    This class provides secure access to secrets stored in Azure Key Vault
    with support for retry logic.

    Note: Caching is handled by SecretField descriptors at the settings level,
    not in this provider. This keeps the provider simple and focused on
    retrieving secrets from Azure Key Vault.

    Attributes:
        kv_settings: Configuration settings for Key Vault
        _secret_client: Lazy-loaded Azure SecretClient instance
    """

    def __init__(self, settings: "KeyVaultSettings"):
        """Initialize Key Vault secrets helper.

        Args:
            settings: Key Vault configuration settings
        """
        self.kv_settings = settings
        self._secret_client: SecretClient | None = None

    @property
    def secret_client(self) -> Optional["SecretClient"]:
        """Get or create Key Vault secret client.

        Returns:
            SecretClient instance or None if not configured
        """
        if self._secret_client is None and self.kv_settings.is_configured:
            from azure.identity import ClientSecretCredential, DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient

            # Use client credentials if provided
            if (
                self.kv_settings.client_id
                and self.kv_settings.client_secret
                and self.kv_settings.tenant_id
            ):
                credential = ClientSecretCredential(
                    tenant_id=self.kv_settings.tenant_id,
                    client_id=self.kv_settings.client_id,
                    client_secret=self.kv_settings.client_secret.get_secret_value(),
                )
            else:
                credential = DefaultAzureCredential()

            self._secret_client = SecretClient(
                vault_url=self.kv_settings.url, credential=credential
            )

        return self._secret_client

    def get_secret(self, secret_name: str, default: str | None = None) -> SecretStr | None:
        """Retrieve a secret from Key Vault.

        Args:
            secret_name: Name of the secret in Key Vault
            default: Default value if the secret cannot be retrieved

        Returns:
            SecretStr containing the secret value, or the default, or None when
            Key Vault is not configured and no default was supplied.

        Raises:
            CTEError: If secret retrieval fails and no default was provided.
                      ``details['error_type']`` names the underlying Azure
                      failure and ``is_retryable`` says whether it was transient.
        """
        if not self.kv_settings.is_configured:
            return SecretStr(default) if default else None

        try:
            return SecretStr(self._fetch_with_retries(secret_name))
        except CTEError as error:
            if default is not None:
                logger.warning(
                    "Falling back to the supplied default for secret '%s': %s",
                    secret_name,
                    error,
                )
                return SecretStr(default)
            raise

    def _fetch_with_retries(self, secret_name: str) -> str:
        """Read one secret, retrying only failures that can succeed later.

        Args:
            secret_name: Name of the secret in Key Vault

        Returns:
            The raw secret value

        Raises:
            CTEError: On a missing client or an exhausted/non-retryable failure
        """
        # `max_retries=0` must still make one attempt; it used to skip the loop
        # entirely and return None, indistinguishable from "secret not found".
        attempts = max(1, self.kv_settings.max_retries)
        retry_delay = self.kv_settings.retry_delay_seconds
        last_error: Exception | None = None
        attempts_made = 0

        for attempt in range(attempts):
            attempts_made = attempt + 1
            try:
                client = self.secret_client
                if client is None:
                    # Key Vault is configured but no client could be built. This
                    # used to `break` silently and return None.
                    raise CTEError(
                        f"Key Vault is configured but no secret client is available; "
                        f"cannot read '{secret_name}'.",
                        error_code=ErrorCode.CONNECTION_ERROR,
                        details={"secret_name": secret_name, "vault_url": self.kv_settings.url},
                    )

                return client.get_secret(secret_name).value

            except Exception as error:
                last_error = error
                if not _is_retryable(error) or attempt == attempts - 1:
                    break

                delay = retry_delay * (2**attempt) + random.uniform(0, retry_delay)
                logger.warning(
                    "Key Vault lookup for '%s' failed with %s; retrying in %.2fs (attempt %d/%d)",
                    secret_name,
                    type(error).__name__,
                    delay,
                    attempts_made,
                    attempts,
                )
                time.sleep(delay)

        if isinstance(last_error, CTEError):
            raise last_error

        raise CTEError(
            f"Failed to retrieve secret '{secret_name}' from Key Vault",
            error_code=ErrorCode.CONNECTION_ERROR,
            details={
                "secret_name": secret_name,
                "error_type": type(last_error).__name__,
                "attempts": attempts_made,
            },
            cause=last_error,
            is_retryable=_is_retryable(last_error),
        ) from last_error
