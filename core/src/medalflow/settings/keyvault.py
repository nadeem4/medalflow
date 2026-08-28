"""Key Vault configuration settings.

This module contains only the configuration settings for Azure Key Vault.
The actual secret retrieval implementation has been moved to the
secret_vault package for better separation of concerns.
"""

from typing import Optional
from pydantic import Field, SecretStr
from pydantic_settings import SettingsConfigDict

from .base import CTEBaseSettings


class KeyVaultSettings(CTEBaseSettings):
    """Configuration settings for Azure Key Vault integration.
    
    This class contains only the configuration needed to connect to
    Azure Key Vault. The actual secret retrieval logic is handled by
    the KeyVaultSecrets provider in the secret_vault package.
    
    Note: Retry settings (max_retries, retry_delay_seconds) are inherited
    from CTEBaseSettings. Caching is handled by SecretField descriptors.
    """
    
    model_config = SettingsConfigDict(
        env_prefix="KEYVAULT_",
        case_sensitive=False
    )
    
    url: Optional[str] = Field(
        None,
        description="Azure Key Vault URL (https://vault-name.vault.azure.net/)"
    )
    use_keyvault: bool = Field(
        default=True,
        description="Whether to use Key Vault for secrets"
    )
    
    client_id: Optional[str] = Field(
        None,
        description="Azure client ID for Key Vault authentication"
    )
    client_secret: Optional[SecretStr] = Field(
        None,
        description="Azure client secret for Key Vault authentication"
    )
    
 
    @property
    def is_configured(self) -> bool:
        """Check if Key Vault is properly configured.
        
        Returns:
            True if Key Vault URL is provided and use_keyvault is enabled
        """
        return bool(self.use_keyvault and self.url)