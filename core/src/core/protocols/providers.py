"""Provider protocol definitions.

This module defines protocols for various provider interfaces used
throughout the MedalFlow framework. These protocols ensure consistent
interfaces for different implementations.
"""

from typing import Protocol, Optional, runtime_checkable, Any, Dict, Callable, List
from pydantic import SecretStr


@runtime_checkable
class SecretProvider(Protocol):
    """Protocol defining the interface for secret providers.
    
    All secret providers must implement this interface to ensure
    compatibility with the MedalFlow settings system.
    
    The protocol is marked as runtime_checkable to allow isinstance()
    checks at runtime, which is useful for validation and testing.
    """
    
    def get_secret(self, secret_name: str, default: Optional[str] = None) -> Optional[SecretStr]:
        """Retrieve a secret value.
        
        Args:
            secret_name: Name of the secret to retrieve
            default: Default value if secret not found
            
        Returns:
            SecretStr containing the secret value, or None if not found
            
        Raises:
            ValueError: If the secret cannot be retrieved and no default is provided
        """
        ...
    
    def clear_cache(self) -> None:
        """Clear any cached secrets.
        
        This method should clear any internal caches to force
        fresh retrieval of secrets on the next access.
        """
        ...
    
    def __getattr__(self, name: str) -> Optional[SecretStr]:
        """Dynamic attribute access for secrets.
        
        This allows accessing secrets using attribute notation,
        e.g., provider.my_secret_name
        
        Args:
            name: Secret name in snake_case format
            
        Returns:
            SecretStr with the secret value or None
        """
        ...


@runtime_checkable
class ConfigProvider(Protocol):
    """Protocol for configuration providers.
    
    This protocol defines the interface for components that provide
    configuration values. This could be environment variables, files,
    remote configuration services, etc.
    """
    
    def get_config(self, key: str, default: Optional[Any] = None) -> Any:
        """Retrieve a configuration value.
        
        Args:
            key: Configuration key to retrieve
            default: Default value if not found
            
        Returns:
            Configuration value or default
        """
        ...
    
    def get_all_configs(self) -> Dict[str, Any]:
        """Retrieve all configuration values.
        
        Returns:
            Dictionary of all configuration key-value pairs
        """
        ...
    
    def refresh(self) -> None:
        """Refresh configuration from source.
        
        This method should reload configuration from the
        underlying source (file, service, etc.).
        """
        ...


@runtime_checkable
class ConfigurationProvider(Protocol):
    """Protocol for configuration providers.
    
    Defines the interface for configuration management systems
    that provide lazy loading and caching of configuration data
    from various sources (e.g., data lake, files, services).
    
    This protocol ensures consistent access to configuration data
    across different layers of the application while maintaining
    clean architecture boundaries.
    
    The protocol is marked as runtime_checkable to allow isinstance()
    checks at runtime, which is useful for validation and testing.
    """
    
    def register_loader(self, name: str, loader: Callable[[], Any]) -> None:
        """Register a configuration loader.
        
        Loaders are functions that return configuration data when called.
        They are typically called lazily when the configuration is first requested.
        
        Args:
            name: Unique name for the configuration
            loader: Callable that returns the configuration data
            
        Example:
            >>> def load_stats_config():
            >>>     return {'tables': ['table1', 'table2']}
            >>> provider.register_loader('stats', load_stats_config)
        """
        ...
    
    def get_configuration(self, name: str) -> Any:
        """Get configuration data, loading if necessary.
        
        If the configuration is not cached, attempts to load it using
        the registered loader. Returns None if no loader is registered
        or if loading fails.
        
        Args:
            name: Name of the configuration to retrieve
            
        Returns:
            Configuration data if available, None otherwise
            
        Example:
            >>> config = provider.get_configuration('stats')
            >>> if config:
            >>>     process_stats(config)
        """
        ...
    
    def warm_all(self) -> Dict[str, bool]:
        """Pre-load all registered configurations.
        
        Attempts to load all registered configurations to warm the cache.
        Useful for application startup to avoid lazy loading delays.
        
        Returns:
            Dictionary mapping configuration names to success status
            
        Example:
            >>> results = provider.warm_all()
            >>> if all(results.values()):
            >>>     print("All configurations loaded successfully")
        """
        ...
    
    def clear_cache(self, name: Optional[str] = None) -> None:
        """Clear cached configurations.
        
        Args:
            name: Specific configuration to clear, or None to clear all
            
        Example:
            >>> provider.clear_cache('stats')  # Clear specific
            >>> provider.clear_cache()  # Clear all
        """
        ...
    
    def is_cached(self, name: str) -> bool:
        """Check if a configuration is cached.
        
        Args:
            name: Name of the configuration
            
        Returns:
            True if configuration is cached, False otherwise
        """
        ...
    
    def get_cached_configurations(self) -> List[str]:
        """Get list of cached configuration names.
        
        Returns:
            List of configuration names that are currently cached
        """
        ...
    
    def get_registered_loaders(self) -> List[str]:
        """Get list of registered loader names.
        
        Returns:
            List of configuration names with registered loaders
        """
        ...