"""Power BI feature manager for dataset refresh configuration.

This module provides the PowerBIManager plugin for managing
Power BI dataset refresh configurations.
"""

from typing import Dict, List, Optional, Any, Callable
import logging
import pandas as pd
from pydantic import BaseModel, Field

from core.core.decorators.features import feature_gate
from core.core.features.base import FeatureManager
from core.core.features.registry import register_feature
from core.core.features import get_feature_manager


logger = logging.getLogger(__name__)


class PowerBIRefreshConfig(BaseModel):
    """Power BI refresh configuration model.
    
    Attributes:
        datasets: Dictionary mapping environment to list of dataset names
    """
    
    datasets: Dict[str, List[str]] = Field(
        default_factory=dict,
        description="Mapping of environment to dataset names"
    )
    
    def get_datasets(self, environment: str = "prod") -> List[str]:
        """Get list of datasets for a specific environment.
        
        Args:
            environment: Environment name (e.g., 'prod', 'dev', 'test')
            
        Returns:
            List of dataset names for the environment
        """
        return self.datasets.get(environment, [])
    
    def get_all_datasets(self) -> List[str]:
        """Get all unique datasets across all environments.
        
        Returns:
            List of all unique dataset names
        """
        all_datasets = set()
        for datasets in self.datasets.values():
            all_datasets.update(datasets)
        return list(all_datasets)
    
    def has_dataset(self, dataset_name: str, environment: Optional[str] = None) -> bool:
        """Check if a dataset exists in configuration.
        
        Args:
            dataset_name: Name of the dataset to check
            environment: Optional environment to check. If None, checks all environments
            
        Returns:
            True if dataset exists in configuration
        """
        if environment:
            return dataset_name in self.get_datasets(environment)
        return dataset_name in self.get_all_datasets()


@feature_gate
class PowerBIManager(FeatureManager):
    """Power BI refresh configuration manager.
    
    Manages Power BI dataset refresh configurations loaded from CSV.
    Uses CacheManager for caching and supports dependency injection
    for data loading from Layer 2 components.
    
    Example:
        >>> powerbi_mgr = get_feature_manager('powerbi')
        >>> if powerbi_mgr:
        >>>     datasets = powerbi_mgr.get_datasets_to_refresh('prod')
        >>>     for dataset in datasets:
        >>>         # Trigger refresh for dataset
        >>>         pass
    """
    
    def __init__(self):
        """Initialize the Power BI manager."""
        super().__init__()
        self._csv_loader: Optional[Callable[[str], pd.DataFrame]] = None
        self._initialized = False
        
    def get_feature_name(self) -> str:
        """Return the feature flag name.
        
        Returns:
            'powerbi' - the feature flag for this manager
        """
        return 'powerbi'
    
    def is_available(self) -> bool:
        """Check if Power BI feature is available.
        
        Always available for Power BI operations.
        
        Returns:
            True - Power BI management is always available
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
        logger.debug("CSV loader injected into PowerBIManager")
    
    def initialize(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Initialize Power BI manager.
        
        Args:
            config: Optional configuration for initialization
        """
        if self._initialized:
            return
            
        self._initialized = True
        
        logger.info("PowerBIManager initialized successfully")
        
        if config:
            logger.debug(f"PowerBIManager initialized with config: {config}")
    
    @property
    def csv_path(self) -> str:
        """Get the CSV path for Power BI configuration.
        
        Returns:
            Path to the Power BI configuration CSV file
        """
        return 'client_configuration/powerbi_refresh_config_prod.csv'
    
    def get_refresh_config(self) -> Optional[PowerBIRefreshConfig]:
        """Get Power BI refresh configuration.
        
        Returns:
            PowerBIRefreshConfig object or None
        """
        cache = get_feature_manager('cache')
        
        if cache:
            return cache.get(
                "powerbi:refresh_config",
                loader=lambda: self._process_refresh_config()
            )
        return self._process_refresh_config()
    
    def get_datasets_to_refresh(self, environment: str = "prod") -> List[str]:
        """Get list of datasets to refresh for an environment.
        
        Args:
            environment: Environment name (e.g., 'prod', 'dev', 'test')
            
        Returns:
            List of dataset names to refresh
        """
        config = self.get_refresh_config()
        if config:
            return config.get_datasets(environment)
        return []
    
    def should_refresh_dataset(self, dataset_name: str, environment: str = "prod") -> bool:
        """Check if a specific dataset should be refreshed.
        
        Args:
            dataset_name: Name of the dataset
            environment: Environment name
            
        Returns:
            True if dataset should be refreshed in this environment
        """
        datasets = self.get_datasets_to_refresh(environment)
        return dataset_name in datasets
    
    def get_all_environments(self) -> List[str]:
        """Get list of all configured environments.
        
        Returns:
            List of environment names
        """
        config = self.get_refresh_config()
        if config:
            return list(config.datasets.keys())
        return []
    
    def _process_refresh_config(self) -> Optional[PowerBIRefreshConfig]:
        """Process CSV into PowerBIRefreshConfig type.
        
        Returns:
            PowerBIRefreshConfig object or None
        """
        if not self._csv_loader:
            logger.warning("No CSV loader injected into PowerBIManager")
            return None
            
        try:
            df = self._csv_loader(self.csv_path)
            if df is None or df.empty:
                logger.debug("No Power BI refresh configuration found")
                return None
            
            # Process into structured type
            datasets = {}
            
            for _, row in df.iterrows():
                dataset_name = row.get('dataset_name')
                environment = row.get('environment', 'prod')
                enabled = row.get('enabled', True)
                
                # Handle string boolean values
                if isinstance(enabled, str):
                    enabled = enabled.lower() in ('true', 'yes', '1', 'on')
                
                if dataset_name and enabled:
                    if environment not in datasets:
                        datasets[environment] = []
                    if dataset_name not in datasets[environment]:
                        datasets[environment].append(dataset_name)
            
            config = PowerBIRefreshConfig(datasets=datasets)
            
            # Log summary
            total_datasets = len(config.get_all_datasets())
            env_summary = ", ".join(
                f"{env}: {len(ds)}" for env, ds in datasets.items()
            )
            logger.info(
                f"Loaded Power BI refresh config: {total_datasets} unique datasets "
                f"across {len(datasets)} environments ({env_summary})"
            )
            
            return config
            
        except Exception as e:
            logger.error(f"Failed to process Power BI refresh config: {e}")
            return None
    
    def get_raw_dataframe(self) -> Optional[pd.DataFrame]:
        """Get the raw Power BI configuration DataFrame.
        
        This method returns the unprocessed CSV data for cases where
        raw access is needed.
        
        Returns:
            DataFrame with raw configuration or None
        """
        cache = get_feature_manager('cache')
        
        if cache:
            return cache.get(
                "powerbi:raw_config",
                loader=lambda: self._load_raw_csv()
            )
        return self._load_raw_csv()
    
    def _load_raw_csv(self) -> Optional[pd.DataFrame]:
        """Load raw CSV without processing.
        
        Returns:
            Raw DataFrame or None
        """
        if not self._csv_loader:
            logger.warning("No CSV loader injected into PowerBIManager")
            return None
            
        try:
            return self._csv_loader(self.csv_path)
        except Exception as e:
            logger.error(f"Failed to load Power BI CSV: {e}")
            return None
    
    def clear_metadata(self) -> None:
        """Clear cached Power BI metadata."""
        cache = get_feature_manager('cache')
        if not cache:
            return
            
        cleared = cache.clear("powerbi:*")
        logger.info(f"Cleared {cleared} Power BI cache entries")
    
    def cleanup(self) -> None:
        """Cleanup resources when shutting down."""
        self._initialized = False
        self._csv_loader = None
        logger.debug("PowerBIManager cleaned up")
    
    @property
    def is_initialized(self) -> bool:
        """Check if the manager is initialized.
        
        Returns:
            True if initialized, False otherwise
        """
        return self._initialized


# Auto-register when module is imported
register_feature('powerbi', PowerBIManager)
logger.debug("PowerBIManager registered with feature registry")