"""Key Vault configuration settings.

This module contains only the configuration settings for Azure Key Vault.
The actual secret retrieval implementation lives in the ``secret_vault``
package.
"""

from pydantic import BaseModel, Field, SecretStr


class KeyVaultSettings(BaseModel):
    """Configuration for Azure Key Vault integration.

    Environment variables are namespaced by the parent settings object, e.g.
    ``MEDALFLOW_KEYVAULT__URL``.

    ``tenant_id`` lives here rather than at the top level because it is used in
    exactly one place: building a :class:`ClientSecretCredential`, alongside
    ``client_id`` and ``client_secret``. Deployments using managed identity
    leave all three unset.
    """

    url: str | None = Field(
        None, description="Azure Key Vault URL (https://vault-name.vault.azure.net/)"
    )
    use_keyvault: bool = Field(default=True, description="Whether to use Key Vault for secrets")

    tenant_id: str | None = Field(
        None,
        description="Azure AD tenant ID (GUID) for service principal auth. "
        "Only needed alongside client_id and client_secret.",
    )
    client_id: str | None = Field(None, description="Azure client ID for Key Vault authentication")
    client_secret: SecretStr | None = Field(
        None, description="Azure client secret for Key Vault authentication"
    )

    max_retries: int = Field(
        default=3, ge=0, le=10, description="Maximum number of retry attempts for secret retrieval"
    )
    retry_delay_seconds: float = Field(
        default=1.0, ge=0.1, le=60.0, description="Delay between retry attempts in seconds"
    )

    @property
    def is_configured(self) -> bool:
        """Check if Key Vault is properly configured.

        Returns:
            True if Key Vault URL is provided and use_keyvault is enabled
        """
        return bool(self.use_keyvault and self.url)
