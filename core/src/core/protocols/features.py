"""Feature manager protocol definitions.

This module defines protocols for feature managers used throughout
the MedalFlow framework.
"""

from typing import Protocol, Optional, Any, Dict, Callable, List
from typing_extensions import runtime_checkable
import pandas as pd


@runtime_checkable
class FeatureManagerProtocol(Protocol):
    """Protocol defining the interface for feature managers.
    
    All feature managers must implement these methods to work
    with the feature system.
    """
    
    def get_feature_name(self) -> str:
        """Return the feature name for this manager.
        
        Returns:
            str: Feature name (e.g., 'cte_stats', 'configuration')
        """
        ...
    
    def is_available(self) -> bool:
        """Check if this feature is currently available/enabled.
        
        Returns:
            bool: True if feature is enabled, False otherwise
        """
        ...
    
    def initialize(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Initialize the manager with optional configuration.
        
        Args:
            config: Optional configuration dictionary for the manager
        """
        ...
    
    def cleanup(self) -> None:
        """Cleanup resources when feature is disabled or app shuts down."""
        ...
    
    


@runtime_checkable
class CacheProtocol(Protocol):
    """Protocol defining cache manager interface.
    
    Any class implementing these methods can be used as a cache manager.
    """
    
    def get(self, key: str, loader: Optional[Callable[[], Any]] = None) -> Any:
        """Get value from cache or load it."""
        ...
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache with optional TTL."""
        ...
    
    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        ...
    
    def delete(self, key: str) -> bool:
        """Remove key from cache."""
        ...
    
    def clear(self, pattern: str = "*") -> int:
        """Clear keys matching pattern."""
        ...
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        ...


@runtime_checkable
class StatsProtocol(Protocol):
    """Protocol defining stats manager interface.
    
    Any class implementing these methods can be used as a stats manager.
    """
    
    def get_stats_columns(self, table_name: str, layer: str = "bronze") -> Optional[List[str]]:
        """Get statistics columns for a specific table."""
        ...
    
    def should_create_stats(self, table_name: str, layer: str = "bronze") -> bool:
        """Check if stats should be created for a table."""
        ...
    
    def get_stats_config(self, schema: str) -> Optional[Any]:
        """Get processed stats configuration for a schema."""
        ...
    
    def get_configured_tables(self, layer: str = "bronze") -> List[str]:
        """Get list of tables configured for statistics."""
        ...
    
    def set_csv_loader(self, loader: Callable[[str], pd.DataFrame]) -> None:
        """Inject CSV loader for reading configuration files."""
        ...
    
    def clear_metadata(self, layer: Optional[str] = None) -> None:
        """Clear cached statistics metadata."""
        ...


@runtime_checkable
class SilverGroupingProtocol(Protocol):
    """Protocol defining silver grouping manager interface.
    
    Any class implementing these methods can be used as a silver grouping manager.
    """
    
    def get_parallel_groups(self, group_name: str) -> List[Any]:
        """Load all parallel groups for a model group."""
        ...
    
    def get_silver_metadata(self) -> Optional[pd.DataFrame]:
        """Get silver metadata configuration."""
        ...
    
    def get_silver_table_config(self) -> Optional[pd.DataFrame]:
        """Get silver table configuration."""
        ...
    
    def get_unique_indexes(self) -> Optional[Dict[str, List[str]]]:
        """Get unique index configuration."""
        ...
    
    def set_csv_loader(self, loader: Callable[[str], pd.DataFrame]) -> None:
        """Inject CSV loader for reading configuration files."""
        ...
    
    def set_json_loader(self, loader: Callable[[str], pd.DataFrame]) -> None:
        """Inject JSON loader for reading group files."""
        ...


@runtime_checkable
class PowerBIProtocol(Protocol):
    """Protocol defining Power BI manager interface.
    
    Any class implementing these methods can be used as a Power BI manager.
    """
    
    def get_refresh_config(self) -> Optional[Any]:
        """Get Power BI refresh configuration."""
        ...
    
    def get_datasets_to_refresh(self, environment: str = "prod") -> List[str]:
        """Get list of datasets to refresh for an environment."""
        ...
    
    def should_refresh_dataset(self, dataset_name: str, environment: str = "prod") -> bool:
        """Check if a dataset should be refreshed."""
        ...
    
    def set_csv_loader(self, loader: Callable[[str], pd.DataFrame]) -> None:
        """Inject CSV loader for reading configuration files."""
        ...


@runtime_checkable
class ClientConfigProtocol(Protocol):
    """Protocol defining client configuration manager interface.
    
    Any class implementing these methods can be used as a client config manager.
    """
    
    def get_product_attributes(self) -> Optional[pd.DataFrame]:
        """Get product attributes configuration."""
        ...
    
    def get_tag_attributes(self) -> Optional[pd.DataFrame]:
        """Get tag attributes configuration."""
        ...
    
    def get_client_uom(self) -> Optional[pd.DataFrame]:
        """Get client unit of measure configuration."""
        ...
    
    def get_to_uom(self) -> Optional[pd.DataFrame]:
        """Get target unit of measure configuration."""
        ...
    
    def set_csv_loader(self, loader: Callable[[str], pd.DataFrame]) -> None:
        """Inject CSV loader for reading configuration files."""
        ...