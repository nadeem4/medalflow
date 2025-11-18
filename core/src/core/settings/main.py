import logging
import os
from typing import Optional, Dict, Any, List, TYPE_CHECKING
from pydantic import Field, SecretStr
from pydantic_settings import SettingsConfigDict

from .compute import ComputeSettings
from .datalake import MultiDataLakeSettings
from core.constants import LayerType
from .keyvault import KeyVaultSettings
from .features import FeatureSettings
from .processing import ProcessingSettings
from .powerbi import PowerBISettings
from .stats import StatsSettings
from .base import CTEBaseSettings
from core.secret_vault.keyvault import KeyVaultSecrets
from core.secret_vault.mock import MockSecrets

if TYPE_CHECKING:
    from core.protocols.providers import SecretProvider


def is_test_mode() -> bool:
    """Check if the application is running in test mode.
    
    Test mode enables special behaviors for testing and development:
    - Mock Key Vault secrets are used instead of real Azure Key Vault
    - Relaxed validation for certain configuration requirements
    - Additional debug logging and diagnostics
    
    Test mode is controlled by the CTE_TEST_MODE environment variable
    and is only allowed in non-production environments.
    
    Returns:
        bool: True if CTE_TEST_MODE="true" (case-insensitive), False otherwise
        
    Example:
        ```python
        if is_test_mode():
            print("Running in test mode - using mock services")
        ```
    """
    return os.getenv("CTE_TEST_MODE", "").lower() == "true"


class _Settings(CTEBaseSettings):
    
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_nested_delimiter="__"  
    )
    
    # Data source fields are now inherited from CTEBaseSettings
    datalake: MultiDataLakeSettings = Field(
        default_factory=MultiDataLakeSettings,
        description="ADLS configuration"
    )
    compute: ComputeSettings = Field(
        default_factory=ComputeSettings,
        description="Compute platform configuration (SQL and Spark engines)"
    )
    keyvault: KeyVaultSettings = Field(
        default_factory=KeyVaultSettings,
        description="Key Vault configuration"
    )
    features: FeatureSettings = Field(
        default_factory=FeatureSettings,
        description="Feature flags configuration"
    )
    processing: ProcessingSettings = Field(
        default_factory=ProcessingSettings,
        description="Data processing configuration"
    )
    powerbi: PowerBISettings = Field(
        default_factory=PowerBISettings,
        description="Power BI integration configuration"
    )
    stats: StatsSettings = Field(
        default_factory=StatsSettings,
        description="Statistics management configuration"
    )
    
    _secret_provider: Optional['SecretProvider'] = None
    
    @property
    def secrets(self) -> Optional['SecretProvider']:
        """Get the secret provider helper with lazy loading.
        
        This property provides access to the secret provider,
        which is created on first access. The provider handles
        retrieving and caching secrets from Azure Key Vault or mock values.
        
        In test mode, a mock implementation is used automatically.
        
        Returns:
            SecretProvider: Instance for retrieving secrets
            
        Example:
            ```python
            # Secrets are loaded automatically when settings are initialized
            settings = get_settings()
            
            # Access loaded secrets through domain settings
            if settings.datalake.processed.auth_method == "access_key":
                # Access key was loaded from Key Vault automatically
                conn_str = settings.datalake.processed.get_connection_string()
            ```
        """
        if self._secret_provider is None:
            self._secret_provider = self._create_secret_provider()
        return self._secret_provider
    
    @property
    def silver_package_name(self) -> str:
        """Generate Python package path for the silver layer.
        
        The package name is computed based on the data source configuration
        and layer type. This enables dynamic import of client-specific
        silver layer transformations.
        
        Returns:
            str: Package path for silver layer modules
            
        Examples:
            - layer_type=CUSTOM, name="fin" -> "custom_fin.silver"
            - layer_type=BASE, name="fin" -> "fin.layers.custom.silver"
        """
        if self.layer_type == LayerType.CUSTOM:
            return f"custom_{self.ds_name}.silver"
        else:
            return f"{self.ds_name}.layers.custom.silver"
    
    @property
    def gold_package_name(self) -> str:
        """Python package path for the gold layer."""
        if self.layer_type == LayerType.CUSTOM:
            return f"custom_{self.ds_name}.gold.gold"
        else:
            return f"{self.ds_name}.layers.custom.gold.gold_query"
    
    @property
    def snapshot_package_name(self) -> str:
        """Python package path for snapshot logic."""
        if self.layer_type == LayerType.CUSTOM:
            return f"custom_{self.ds_name}.snapshot.snapshot"
        else:
            return f"{self.ds_name}.layers.custom.snapshot.snapshot"
    
    @property
    def dimension_package_name(self) -> str:
        """Python package path for dimension tables."""
        if self.layer_type == LayerType.CUSTOM:
            return f"custom_{self.ds_name}.silver.dimension_table.dimension_tables"
        else:
            return f"{self.ds_name}.layers.custom.silver.dimension_table.dimension_tables"
    
    @property
    def silver_proc_mapping_package_name(self) -> str:
        """Python package path for silver stored procedure mappings."""
        if self.layer_type == LayerType.CUSTOM:
            return f"custom_{self.ds_name}.config.python_proc"
        else:
            return f"{self.ds_name}.config.python_proc"
    
    @property
    def silver_proc_crud_mapping_package_name(self) -> str:
        """Python package path for silver CRUD mappings."""
        if self.layer_type == LayerType.CUSTOM:
            return f"custom_{self.ds_name}.config.crud_mapping"
        else:
            return f"{self.ds_name}.config.crud_mapping"
    
    def model_post_init(self, __context) -> None:
        """Post initialization hook to set up cross-references and attach secret providers.
        
        This method is called after Pydantic has validated all fields. It performs
        additional initialization including:
        
        1. Setting up cross-references between settings components
        2. Creating appropriate secret provider (KeyVault or mock)
        3. Attaching the provider to all components that need secrets
        
        The initialization is designed to be fault-tolerant - errors in secret
        provider creation are logged but don't prevent the settings from being created.
        
        Args:
            __context: Pydantic context (internal use)
        """
        super().model_post_init(__context)
   
        
        secret_provider = self._create_secret_provider()
        if secret_provider:
            self._attach_secret_provider(secret_provider)
    
    
    def _create_secret_provider(self) -> Optional['SecretProvider']:
        """Create the appropriate secret provider based on configuration.
        
        This method determines which secret provider to use:
        1. Real KeyVault if configured
        2. Mock provider if in test mode
        3. None if neither
        
        Returns:
            Secret provider instance or None
        """
        logger = logging.getLogger(__name__)
        
        if self.keyvault.is_configured:
            try:
                # Production: Use real KeyVault
                logger.debug("Creating KeyVault secret provider")
                return KeyVaultSecrets(settings=self.keyvault)
            except Exception as e:
                logger.warning(f"Failed to create KeyVault provider: {e}")
                return None
                
        elif is_test_mode():
            # Test mode: Create mock provider
            logger.info("Using mock secret provider for test mode")
            return MockSecrets()
            
        return None

    def _attach_secret_provider(self, provider: Any) -> None:
        """Attach secret provider to all settings components.
        
        This method attaches the secret provider to all components
        that support the attach_secrets() method. Components can
        then lazy-load secrets as needed.
        
        All components have been refactored to use the new pattern,
        so we can now directly attach without checking.
        
        Args:
            provider: Secret provider instance (KeyVault or mock)
        """
        logger = logging.getLogger(__name__)
        
        # Attach to self (main settings) - for base SP credentials
        try:
            super().attach_secrets(provider)
            logger.debug("Attached secret provider to main settings")
        except Exception as e:
            logger.warning(f"Failed to attach secrets to main settings: {e}")
        
        # Attach to compute settings
        try:
            self.compute.attach_secrets(provider)
            logger.debug("Attached secret provider to compute settings")
        except Exception as e:
            logger.warning(f"Failed to attach secrets to compute: {e}")
        
        # Attach to datalake settings
        try:
            self.datalake.attach_secrets(provider)
            logger.debug("Attached secret provider to datalake settings")
        except Exception as e:
            logger.warning(f"Failed to attach secrets to datalake: {e}")
        
        # Attach to powerbi settings
        try:
            self.powerbi.attach_secrets(provider)
            logger.debug("Attached secret provider to powerbi settings")
        except Exception as e:
            logger.warning(f"Failed to attach secrets to powerbi: {e}")
        
    



# Singleton instance
_settings: Optional[_Settings] = None


def get_settings(force_reload: bool = False) -> _Settings:
    """Get the singleton settings instance for the application.
    
    This function implements a thread-safe singleton pattern to ensure
    that only one Settings instance exists throughout the application
    lifecycle. The settings are loaded from environment variables and
    Key Vault on first access.
    
    The singleton pattern is important because:
    1. Settings loading can be expensive (Key Vault access)
    2. Consistent configuration across all components
    3. Centralized configuration management
    
    Args:
        force_reload: If True, creates a new Settings instance even if
                     one already exists. Useful for testing or when
                     environment variables have changed.
        
    Returns:
        Settings: The singleton Settings instance
        
    Example:
        ```python
        # First call loads settings
        settings = get_settings()
        
        # Subsequent calls return same instance
        settings2 = get_settings()
        assert settings is settings2
        
        # Force reload to pick up environment changes
        new_settings = get_settings(force_reload=True)
        assert new_settings is not settings
        ```
        
    Note:
        This function is thread-safe for reading but not for the initial
        creation. In practice, settings are typically loaded once at
        application startup before threading begins.
    """
    global _settings
    
    if _settings is None or force_reload:
        _settings = _Settings()
    
    return _settings


def _reload_settings() -> _Settings:
    """Force reload of settings.
    
    This is primarily for testing purposes where you need to reset
    the singleton instance.
    
    Returns:
        A fresh _Settings instance
    """
    global _settings
    _settings = None
    return get_settings(force_reload=True)
