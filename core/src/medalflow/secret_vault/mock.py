"""Mock secret provider for testing and development.

This module provides the MockSecrets class which implements the
SecretProvider protocol for testing without requiring actual Key Vault access.
"""

from typing import Optional

from pydantic import SecretStr


class MockSecrets:
    """Mock secret provider for testing and development.
    
    This provider returns predefined test values for secrets,
    useful for development and testing without requiring actual
    KeyVault access.
    
    Attributes:
        mock_values: Dictionary mapping secret names to mock values
    """
    
    def __init__(self, mock_values: Optional[dict[str, str]] = None):
        """Initialize mock secret provider.
        
        Args:
            mock_values: Dictionary of secret_name -> value mappings.
                        If None, default mock values are used.
        """
        self.mock_values = mock_values or self._get_default_mocks()
    
    def _get_default_mocks(self) -> dict[str, str]:
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
        value = self.mock_values.get(secret_name, default)
        
        return SecretStr(value) if value is not None else None
    
    def clear_cache(self) -> None:
        """Clear the secret cache.
        
        Mock secrets are not cached, so this is a no-op kept to satisfy
        the SecretProvider protocol.
        """
    
    def add_mock_secret(self, secret_name: str, value: str) -> None:
        """Add or update a mock secret value.
        
        This is useful for tests that need specific secret values.
        
        Args:
            secret_name: Name of the secret to add/update
            value: The mock value for the secret
        """
        self.mock_values[secret_name] = value
