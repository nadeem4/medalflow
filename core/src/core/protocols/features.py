"""Feature manager protocol definitions.

This module defines protocols for feature managers used throughout
the MedalFlow framework.
"""

from typing import Protocol, Optional, Any, Dict, Callable, List
from typing_extensions import runtime_checkable
import pandas as pd


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
