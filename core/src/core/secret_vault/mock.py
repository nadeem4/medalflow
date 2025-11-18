"""Mock secret provider for testing and development.

This module provides the MockSecrets class which implements the
SecretProvider protocol for testing without requiring actual Key Vault access.
"""

from typing import Optional, Dict
from pydantic import SecretStr


class MockSecrets:
    """Mock secret provider for testing and development.
    
    This provider returns predefined test values for secrets,
    useful for development and testing without requiring actual
    KeyVault access.
    
    Attributes:
        mock_values: Dictionary mapping secret names to mock values
        _cache: Internal cache (maintained for protocol compatibility)
    """
    
    def __init__(self, mock_values: Optional[Dict[str, str]] = None):
        """Initialize mock secret provider.
        
        Args:
            mock_values: Dictionary of secret_name -> value mappings.
                        If None, default mock values are used.
        """
        self.mock_values = mock_values or self._get_default_mocks()
        self._cache: Dict[str, SecretStr] = {}
    
    def _get_default_mocks(self) -> Dict[str, str]:
        """Get default mock values for common secrets.
        
        Returns:
            Dictionary of default secret name to value mappings
        """
        return {
            # Synapse/SQL Server secrets
            "ETL-SERVER": "Server=test-etl;Database=etl;Trusted_Connection=yes;",
            "ETL-SYNAPSE": "Server=test-etl-synapse.sql.azuresynapse.net;Database=etl;",
            "CONSUMPTION-SERVER": "Server=test-consumption;Database=consumption;Trusted_Connection=yes;",
            "CONSUMPTION-SYNAPSE": "Server=test-consumption-synapse.sql.azuresynapse.net;Database=consumption;",
            "SYN-DB-MASTER-KEY": "mock-master-key-xxxxx",
            
            # Data Lake secrets
            "PROCESSED-ADLS-ACCOUNT-KEY": "mock_processed_key_xxxxx",
            "CMAA-CONTENT-ADLS-ACCESS-KEY": "mock_internal_key_xxxxx",
            "PROCESSED-ADLS-ACCESS-KEY": "mock_processed_access_key_xxxxx",
            "INTERNAL-ADLS-ACCESS-KEY": "mock_internal_access_key_xxxxx",
            
            # Service Principal secrets
            "SP-CLIENT-ID": "mock-client-id-00000000-0000-0000-0000-000000000000",
            "SP-CLIENT-SECRET": "mock-client-secret-xxxxx",
            "TENANT-ID": "mock-tenant-00000000-0000-0000-0000-000000000000",
            
            # Power BI secrets
            "POWERBI-CLIENT-ID": "mock-powerbi-client-00000000-0000-0000-0000-000000000000",
            "POWERBI-CLIENT-SECRET": "mock-powerbi-secret-xxxxx",
            
            # Additional common secrets
            "API-KEY": "mock-api-key-xxxxx",
            "DATABASE-PASSWORD": "mock-db-password-xxxxx",
        }
    
    def get_secret(self, secret_name: str, default: Optional[str] = None) -> Optional[SecretStr]:
        """Get a mock secret value.
        
        Args:
            secret_name: Name of the secret
            default: Default value if secret not found
            
        Returns:
            SecretStr with mock value or default
        """
        # Check if we have a cached value first (for consistency with KeyVault provider)
        if secret_name in self._cache:
            return self._cache[secret_name]
        
        # Get from mock values
        value = self.mock_values.get(secret_name, default)
        
        if value is not None:
            secret_str = SecretStr(value)
            # Cache it for consistency
            self._cache[secret_name] = secret_str
            return secret_str
        
        return None
    
    def clear_cache(self) -> None:
        """Clear the secret cache.
        
        This method clears the internal cache to maintain
        protocol compatibility, though for mock secrets this
        has minimal effect.
        """
        self._cache.clear()
    
    def add_mock_secret(self, secret_name: str, value: str) -> None:
        """Add or update a mock secret value.
        
        This is useful for tests that need specific secret values.
        
        Args:
            secret_name: Name of the secret to add/update
            value: The mock value for the secret
        """
        self.mock_values[secret_name] = value
        # Clear cache to ensure the new value is used
        if secret_name in self._cache:
            del self._cache[secret_name]
    
    def __getattr__(self, name: str) -> Optional[SecretStr]:
        """Dynamic attribute access for secrets.
        
        This allows accessing any secret using attribute notation,
        e.g., provider.etl_server or provider.api_key
        
        Args:
            name: Secret name in snake_case format
            
        Returns:
            SecretStr with the secret value or None
        """
        # Convert snake_case to KEBAB-CASE for lookup
        # e.g., etl_server -> ETL-SERVER
        kebab_name = name.upper().replace('_', '-')
        return self.get_secret(kebab_name)