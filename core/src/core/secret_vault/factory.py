"""Factory for creating secret providers.

This module provides factory functions for creating the appropriate
secret provider based on configuration and environment.
"""

from typing import Optional, Dict, TYPE_CHECKING
import os

from core.protocols.providers import SecretProvider
from .keyvault import KeyVaultSecrets
from .mock import MockSecrets

if TYPE_CHECKING:
    from core.settings.keyvault import KeyVaultSettings


def is_test_mode() -> bool:
    """Check if the application is running in test mode.
    
    Returns:
        True if CTE_TEST_MODE="true" (case-insensitive), False otherwise
    """
    return os.getenv("CTE_TEST_MODE", "").lower() == "true"


def create_secret_provider(
    keyvault_settings: Optional['KeyVaultSettings'] = None,
    mock_values: Optional[Dict[str, str]] = None,
    force_mock: bool = False
) -> SecretProvider:
    """Create the appropriate secret provider based on configuration.
    
    This factory function creates either a KeyVault provider or a Mock provider
    based on the configuration and environment settings.
    
    Args:
        keyvault_settings: Optional KeyVault configuration settings
        mock_values: Optional dictionary of mock secret values
        force_mock: If True, always create a mock provider
        
    Returns:
        SecretProvider implementation (KeyVaultSecrets or MockSecrets)
        
    Example:
        >>> from core.settings.keyvault import KeyVaultSettings
        >>> from core.secret_vault import create_secret_provider
        >>> 
        >>> # Production: Use KeyVault
        >>> kv_settings = KeyVaultSettings(url="https://vault.vault.azure.net/")
        >>> provider = create_secret_provider(kv_settings)
        >>> 
        >>> # Testing: Use Mock
        >>> test_secrets = {"API-KEY": "test-key"}
        >>> provider = create_secret_provider(mock_values=test_secrets, force_mock=True)
    """
    # Determine if we should use mock provider
    use_mock = force_mock or is_test_mode()
    
    # If not forcing mock and we have KeyVault settings, check if it's configured
    if not use_mock and keyvault_settings and keyvault_settings.is_configured():
        return KeyVaultSecrets(keyvault_settings)
    
    # Default to mock provider
    return MockSecrets(mock_values)

