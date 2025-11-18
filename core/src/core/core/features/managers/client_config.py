"""Client configuration feature manager for generic CSV configs.

This module provides the ClientConfigManager plugin for managing
generic client configurations that don't require special processing.
"""

from typing import Optional, Any, Dict, Callable
import logging
import pandas as pd

from core.core.decorators.features import feature_gate
from core.core.features.base import FeatureManager
from core.core.features.registry import register_feature
from core.core.features import get_feature_manager


logger = logging.getLogger(__name__)


@feature_gate
class ClientConfigManager(FeatureManager):
    """Generic client configuration manager for simple CSV configs.
    
    Handles configurations that don't need special processing,
    just returning DataFrames. This includes:
    - Product attributes
    - Tag attributes
    - Client UOM conversions
    - Target UOM conversions
    
    Uses CacheManager for caching and supports dependency injection
    for data loading from Layer 2 components.
    
    Example:
        >>> client_mgr = get_feature_manager('client_config')
        >>> if client_mgr:
        >>>     product_df = client_mgr.get_product_attributes()
        >>>     tag_df = client_mgr.get_tag_attributes()
        >>>     # Use DataFrames for processing
    """
    
    def __init__(self):
        """Initialize the client config manager."""
        super().__init__()
        self._csv_loader: Optional[Callable[[str], pd.DataFrame]] = None
        self._initialized = False
        
    def get_feature_name(self) -> str:
        """Return the feature flag name.
        
        Returns:
            'client_config' - the feature flag for this manager
        """
        return 'client_config'
    
    def is_available(self) -> bool:
        """Check if client config feature is available.
        
        Always available for client configuration access.
        
        Returns:
            True - Client configuration is always available
        """
        return True
    
    def set_csv_loader(self, loader: Callable[[str], pd.DataFrame]) -> None:
        """Inject CSV loader function (dependency injection).
        
        This allows Layer 2 components to provide data loading capability
        without creating circular dependencies.
        
        Args:
            loader: Function that takes a path and returns a DataFrame
        """
        self._csv_loader = loader
        logger.debug("CSV loader injected into ClientConfigManager")
    
    def initialize(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Initialize client config manager.
        
        Args:
            config: Optional configuration for initialization
        """
        if self._initialized:
            return
            
        self._initialized = True
        
        logger.info("ClientConfigManager initialized successfully")
        
        if config:
            logger.debug(f"ClientConfigManager initialized with config: {config}")
    
    def get_product_attributes(self) -> Optional[pd.DataFrame]:
        """Get product attributes configuration.
        
        Returns:
            DataFrame with product attributes or None
        """
        return self._get_config(
            'product_attr',
            'client_configuration/product_attr.csv'
        )
    
    def get_tag_attributes(self) -> Optional[pd.DataFrame]:
        """Get tag attributes configuration.
        
        Returns:
            DataFrame with tag attributes or None
        """
        return self._get_config(
            'tag_attr',
            'client_configuration/tag_attr.csv'
        )
    
    def get_client_uom(self) -> Optional[pd.DataFrame]:
        """Get client UOM conversion configuration.
        
        Returns:
            DataFrame with client UOM conversions or None
        """
        return self._get_config(
            'client_uom',
            'client_configuration/client_uom.csv'
        )
    
    def get_to_uom(self) -> Optional[pd.DataFrame]:
        """Get target UOM conversion configuration.
        
        Returns:
            DataFrame with target UOM conversions or None
        """
        return self._get_config(
            'to_uom',
            'client_configuration/to_uom.csv'
        )
    
    def get_uom_conversions(self) -> Dict[str, Optional[pd.DataFrame]]:
        """Get both UOM conversion configurations.
        
        Returns:
            Dictionary with 'client_uom' and 'to_uom' DataFrames
        """
        return {
            'client_uom': self.get_client_uom(),
            'to_uom': self.get_to_uom()
        }
    
    def _get_config(self, config_name: str, path: str) -> Optional[pd.DataFrame]:
        """Get configuration DataFrame with caching.
        
        Args:
            config_name: Name for cache key
            path: Path to CSV file
            
        Returns:
            DataFrame or None
        """
        cache = get_feature_manager('cache')
        
        if cache:
            return cache.get(
                f"client:{config_name}",
                loader=lambda: self._load_csv(path)
            )
        return self._load_csv(path)
    
    def _load_csv(self, path: str) -> Optional[pd.DataFrame]:
        """Load CSV using injected loader.
        
        Args:
            path: Path to CSV file
            
        Returns:
            DataFrame or None
        """
        if not self._csv_loader:
            logger.warning("No CSV loader injected into ClientConfigManager")
            return None
            
        try:
            df = self._csv_loader(path)
            if df is not None and not df.empty:
                logger.debug(f"Loaded {path}: {len(df)} rows, {len(df.columns)} columns")
            return df
        except FileNotFoundError:
            logger.debug(f"Configuration file not found: {path}")
            return None
        except Exception as e:
            logger.error(f"Failed to load {path}: {e}")
            return None
    
    
    
    def clear_metadata(self, config_name: Optional[str] = None) -> None:
        """Clear cached client configuration metadata.
        
        Args:
            config_name: Optional specific config to clear. If None, clears all
        """
        cache = get_feature_manager('cache')
        if not cache:
            return
            
        if config_name:
            # Clear specific config
            cleared = cache.delete(f"client:{config_name}")
            if cleared:
                logger.info(f"Cleared cache for client config: {config_name}")
        else:
            # Clear all client configs
            cleared = cache.clear("client:*")
            logger.info(f"Cleared {cleared} client config cache entries")
    
    def cleanup(self) -> None:
        """Cleanup resources when shutting down."""
        self._initialized = False
        self._csv_loader = None
        logger.debug("ClientConfigManager cleaned up")
    
    @property
    def is_initialized(self) -> bool:
        """Check if the manager is initialized.
        
        Returns:
            True if initialized, False otherwise
        """
        return self._initialized


# Auto-register when module is imported
register_feature('client_config', ClientConfigManager)
logger.debug("ClientConfigManager registered with feature registry")