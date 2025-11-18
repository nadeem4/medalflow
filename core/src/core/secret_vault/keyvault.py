"""Azure KeyVault secret provider implementation.

This module provides the KeyVaultSecrets class which implements the
SecretProvider protocol for retrieving secrets from Azure Key Vault.
"""

from typing import Optional, TYPE_CHECKING
import time
from pydantic import SecretStr

if TYPE_CHECKING:
    from azure.keyvault.secrets import SecretClient
    from core.settings.keyvault import KeyVaultSettings


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
    
    def __init__(self, settings: 'KeyVaultSettings'):
        """Initialize Key Vault secrets helper.
        
        Args:
            settings: Key Vault configuration settings
        """
        self.kv_settings = settings
        self._secret_client: Optional['SecretClient'] = None
    
    @property
    def secret_client(self) -> Optional['SecretClient']:
        """Get or create Key Vault secret client.
        
        Returns:
            SecretClient instance or None if not configured
        """
        if self._secret_client is None and self.kv_settings.is_configured():
            from azure.keyvault.secrets import SecretClient
            from azure.identity import DefaultAzureCredential, ClientSecretCredential
            
            # Use client credentials if provided
            if self.kv_settings.client_id and self.kv_settings.client_secret and self.kv_settings.tenant_id:
                credential = ClientSecretCredential(
                    tenant_id=self.kv_settings.tenant_id,
                    client_id=self.kv_settings.client_id,
                    client_secret=self.kv_settings.client_secret.get_secret_value() 
                )
            else:
                credential = DefaultAzureCredential()
            
            self._secret_client = SecretClient(
                vault_url=self.kv_settings.url,
                credential=credential
            )
        
        return self._secret_client
    
    def get_secret(self, secret_name: str, default: Optional[str] = None) -> Optional[SecretStr]:
        """Retrieve a secret from Key Vault.
        
        Args:
            secret_name: Name of the secret in Key Vault
            default: Default value if secret not found
            
        Returns:
            SecretStr containing the secret value or None
            
        Raises:
            ValueError: If secret retrieval fails and no default provided
        """
        if not self.kv_settings.is_configured():
            return SecretStr(default) if default else None
        
        try:
            max_retries = self.kv_settings.max_retries
            retry_delay = self.kv_settings.retry_delay_seconds
            
            for attempt in range(max_retries):
                try:
                    if self.secret_client:
                        secret = self.secret_client.get_secret(secret_name)
                        return SecretStr(secret.value)
                    else:
                        break
                        
                except Exception as e:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    raise
                    
        except Exception as e:
            if default is not None:
                return SecretStr(default)
            raise ValueError(f"Failed to retrieve secret '{secret_name}': {str(e)}")
        
        return SecretStr(default) if default else None
    