"""Data lake configuration loading service.

This module provides the DataLakeConfigurationService which injects
data loading capabilities into feature managers via dependency injection.
"""

from typing import Dict, Any, Optional, List
import logging

from core.datalake import get_internal_datalake_client
from core.core.features import get_feature_manager
from core.logging import get_logger


logger = logging.getLogger(__name__)


class DataLakeConfigurationService:
    """Service for injecting data lake access into feature managers.
    
    This service is responsible for:
    - Providing data loading capabilities to Layer 1 managers
    - Injecting CSV and JSON loaders via dependency injection
    - Maintaining clean architecture without circular dependencies
    
    It acts as the bridge between Layer 2 (data lake access) and
    Layer 1 (feature managers), using dependency injection to avoid
    circular dependencies.
    
    Example:
        >>> service = get_configuration_service()
        >>> service.initialize()  # Injects loaders into managers
        >>> # Now managers can load data from data lake
    """
    
    _instance = None
    
    def __new__(cls):
        """Ensure singleton pattern for configuration service."""
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the configuration service."""
        # Only initialize once (singleton pattern)
        if hasattr(self, '_initialized'):
            return
        
        self.logger = get_logger(self.__class__.__name__)
        self.client = get_internal_datalake_client()
        self._initialized = False
        self._injected_managers = []
    
    def initialize(self) -> bool:
        """Initialize and inject dependencies into all managers.
        
        This method injects data loading functions into all feature managers
        that need them, enabling them to load data from the data lake without
        creating circular dependencies.
        
        Returns:
            True if initialization successful, False otherwise
        """
        if self._initialized:
            self.logger.debug("Configuration service already initialized")
            return True
        
        try:
            # Inject into all managers that need CSV loading
            csv_managers = [
                'stats',           # StatsManager
                'powerbi',        # PowerBIManager
                'client_config'   # ClientConfigManager
            ]
            
            for manager_name in csv_managers:
                mgr = get_feature_manager(manager_name)
                if mgr and hasattr(mgr, 'set_csv_loader'):
                    mgr.set_csv_loader(self.client.read_csv)
                    self._injected_managers.append(f"{manager_name}:csv")
                    self.logger.info(f"Injected CSV loader into {manager_name} manager")
                elif mgr:
                    self.logger.debug(f"{manager_name} manager doesn't have set_csv_loader")
                else:
                    self.logger.debug(f"{manager_name} manager not available")
            
            # No longer need to inject JSON loader for silver grouping (removed)
            
            self._initialized = True
            self.logger.info(
                f"Configuration service initialized with {len(self._injected_managers)} injections"
            )
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize configuration service: {e}")
            return False
    
    def warm_all(self) -> Dict[str, bool]:
        """Warm all configuration caches by triggering loads.
        
        This method attempts to load common configurations to warm
        the caches, avoiding lazy loading delays during runtime.
        
        Returns:
            Dictionary mapping configuration names to success status
        """
        # Ensure initialized
        if not self.initialize():
            return {"initialized": False}
        
        results = {}
        
        # Warm stats for all schemas
        stats_mgr = get_feature_manager('stats')
        if stats_mgr:
            for schema in ['bronze', 'silver', 'gold']:
                try:
                    config = stats_mgr.get_stats_config(schema)
                    results[f'stats_{schema}'] = config is not None
                except Exception as e:
                    self.logger.error(f"Failed to warm stats for {schema}: {e}")
                    results[f'stats_{schema}'] = False
        
        # Silver grouping has been deprecated - transformations now use
        # ExecutionPlanOrchestrator for dependency-based execution
        
        # Warm PowerBI config
        powerbi_mgr = get_feature_manager('powerbi')
        if powerbi_mgr:
            try:
                config = powerbi_mgr.get_refresh_config()
                results['powerbi_refresh'] = config is not None
            except Exception as e:
                self.logger.error(f"Failed to warm PowerBI config: {e}")
                results['powerbi_refresh'] = False
        
        # Warm client configs
        client_mgr = get_feature_manager('client_config')
        if client_mgr:
            try:
                results['product_attr'] = client_mgr.get_product_attributes() is not None
                results['tag_attr'] = client_mgr.get_tag_attributes() is not None
                results['client_uom'] = client_mgr.get_client_uom() is not None
                results['to_uom'] = client_mgr.get_to_uom() is not None
            except Exception as e:
                self.logger.error(f"Failed to warm client configs: {e}")
        
        self.logger.info(f"Cache warming results: {results}")
        return results
    
    def get_injected_managers(self) -> List[str]:
        """Get list of managers that have been injected with loaders.
        
        Returns:
            List of manager:loader_type strings (e.g., 'stats:csv')
        """
        return self._injected_managers.copy()
    
    def reinitialize(self) -> bool:
        """Force reinitialization of the service.
        
        This clears the initialized state and reinjects all loaders.
        Useful for testing or after configuration changes.
        
        Returns:
            True if reinitialization successful
        """
        self._initialized = False
        self._injected_managers = []
        return self.initialize()
    
    def __repr__(self) -> str:
        """String representation of the service."""
        return (
            f"DataLakeConfigurationService("
            f"initialized={self._initialized}, "
            f"injections={len(self._injected_managers)})"
        )


# Singleton accessor
def get_configuration_service() -> DataLakeConfigurationService:
    """Get the singleton DataLakeConfigurationService instance.
    
    Returns:
        The DataLakeConfigurationService singleton instance
        
    Example:
        >>> service = get_configuration_service()
        >>> service.initialize()
        >>> results = service.warm_all()
    """
    return DataLakeConfigurationService()