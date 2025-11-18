"""Statistics feature manager.

This module provides the StatsManager plugin for managing database
statistics configuration and operations across all application layers.
"""

from typing import Dict, List, Optional, Any, Callable, TYPE_CHECKING
import logging
import pandas as pd

from core.core.decorators.features import feature_gate
from core.core.features.base import FeatureManager
from core.core.features.registry import register_feature
from core.core.features import get_feature_manager
from core.settings import get_settings
from core.protocols.features import StatsProtocol, CacheProtocol
from ...types import StatsConfiguration


logger = logging.getLogger(__name__)


@feature_gate
class StatsManager(StatsProtocol, FeatureManager):
    """Statistics configuration manager with business logic.
    
    Manages database statistics configuration across all layers
    (compute, medallion, datalake, etc.). This manager provides:
    - Table-level statistics configuration
    - Column-specific statistics metadata
    - Cross-layer statistics coordination
    - CSV data loading with dependency injection
    
    Uses CacheManager for caching and supports dependency injection
    for data loading from Layer 2 components.
    
    Example:
        >>> stats_mgr = get_feature_manager('stats')
        >>> if stats_mgr:
        >>>     columns = stats_mgr.get_stats_columns('InventTrans', 'bronze')
        >>>     if columns:
        >>>         # Create statistics on columns
        >>>         pass
    """
    
    def __init__(self):
        """Initialize the stats manager."""
        super().__init__()
        self._csv_loader: Optional[Callable[[str], pd.DataFrame]] = None
        self._initialized = False
    
    def get_feature_name(self) -> str:
        """Return the feature flag name.
        
        Returns:
            'stats' - the feature flag that controls this manager
        """
        return 'stats'
    
    def is_available(self) -> bool:
        """Check if stats feature is enabled.
        
        Returns:
            True if cte_stats_enabled is True in settings
        """
        return self.feature_settings.cte_stats_enabled
    
    def set_csv_loader(self, loader: Callable[[str], pd.DataFrame]) -> None:
        """Inject CSV loader function (dependency injection).
        
        This allows Layer 2 components to provide data loading capability
        without creating circular dependencies.
        
        Args:
            loader: Function that takes a path and returns a DataFrame
        """
        self._csv_loader = loader
        logger.debug("CSV loader injected into StatsManager")
    
    def initialize(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Initialize stats manager.
        
        Sets up the manager for operation.
        
        Args:
            config: Optional configuration for initialization
        """
        if self._initialized:
            return
            
        self._initialized = True
        
        logger.info("StatsManager initialized successfully")
        
        if config:
            logger.debug(f"StatsManager initialized with config: {config}")
    
    @property
    def csv_path(self) -> str:
        """Get the CSV path from settings.
        
        Returns:
            Path to the statistics configuration CSV file
        """
        settings = get_settings()
        return settings.stats.stats_csv_path   
    
    def get_stats_config(self, schema: str) -> Optional['StatsConfiguration']:
        """Get processed stats configuration for a schema.
        
        Args:
            schema: Schema name ('bronze', 'silver', 'gold')
            
        Returns:
            StatsConfiguration object or None
        """
        cache: Optional[CacheProtocol] = get_feature_manager('cache')
        
        if cache:
            return cache.get(
                f"stats:{schema}",
                loader=lambda: self._process_stats(schema)
            )
        return self._process_stats(schema)
    
    def get_stats_columns(self, table_name: str, layer: str = "bronze") -> Optional[List[str]]:
        """Get statistics columns for a specific table.
        
        Args:
            table_name: Name of the table
            layer: Data layer ('bronze', 'silver', etc.)
            
        Returns:
            List of column names if defined, None otherwise
        """
        config = self.get_stats_config(layer)
        if config:
            return config.get_table_columns(table_name.lower())
        return None

    
    def _process_stats(self, schema: str) -> Optional['StatsConfiguration']:
        """Process raw CSV into StatsConfiguration.
        
        Args:
            schema: Schema name to filter for
            
        Returns:
            StatsConfiguration object or None
        """
        if not self._csv_loader:
            logger.warning("No CSV loader injected into StatsManager")
            return None
            
        try:
           
            settings = get_settings()
            df = self._csv_loader(self.csv_path)
            if df is None or df.empty:
                logger.debug(f"No stats configuration found for schema: {schema}")
                return None
            
            df['schema_name'] = df['schema_name'].str.lower()
            df['table_name'] = df['table_name'].str.lower().replace(settings.table_prefix, '')
            df['stats_column_name'] = df['stats_column_name'].str.lower()
                
            # Filter for the specified schema
            schema_df = df[df['schema_name'] == schema]
            
            if schema_df.empty:
                logger.debug(f"No stats entries for schema: {schema}")
                return StatsConfiguration(schema_name=schema, table_stats={})
            
            # Build stats dictionary
            stats_dict = {}
            for table_name, group in schema_df.groupby('table_name'):
                stats_dict[table_name] = group['stats_column_name'].tolist()
                
            config = StatsConfiguration(
                schema_name=schema,
                table_stats=stats_dict
            )
            
            logger.info(
                f"Loaded stats configuration for {schema}: "
                f"{len(stats_dict)} tables, "
                f"{config.get_total_columns()} columns"
            )
            
            return config
            
        except Exception as e:
            logger.error(f"Failed to process stats for {schema}: {e}")
            return None
    

    
    def clear_metadata(self, layer: Optional[str] = None) -> None:
        """Clear cached statistics metadata.
        
        Args:
            layer: Optional layer to clear. If None, clears all metadata
        """
        cache = get_feature_manager('cache')
        if not cache:
            return
            
        if layer:
            # Clear specific layer
            cleared = cache.clear(f"stats:{layer}*")
            logger.info(f"Cleared {cleared} stats cache entries for layer: {layer}")
        else:
            # Clear all stats
            cleared = cache.clear("stats:*")
            logger.info(f"Cleared {cleared} stats cache entries")
    
    def cleanup(self) -> None:
        """Cleanup resources when shutting down."""
        self._initialized = False
        self._csv_loader = None
        logger.debug("StatsManager cleaned up")
    
    @property
    def is_initialized(self) -> bool:
        """Check if the manager is initialized.
        
        Returns:
            True if initialized, False otherwise
        """
        return self._initialized


# Auto-register when module is imported
register_feature('stats', StatsManager)
logger.debug("StatsManager registered with feature registry")