"""Cache feature manager for global in-memory caching.

This module provides the CacheManager plugin for centralized caching
of all configuration and data across the application, similar to Redis/Memcache.
"""

from typing import Dict, Any, Optional, Callable
import time
import fnmatch
import logging

from core.core.decorators.features import feature_gate
from core.core.features.base import FeatureManager
from core.core.features.registry import register_feature

from core.protocols import CacheProtocol


logger = logging.getLogger(__name__)


@feature_gate
class CacheManager(CacheProtocol, FeatureManager):
    """Global cache feature manager (like Redis/Memcache).
    
    Provides in-memory caching with TTL support, pattern matching,
    and namespace support. The cache is only instantiated when
    caching is enabled via the configuration_cache_enabled setting.
    
    Example:
        >>> cache_mgr = get_feature_manager('cache')
        >>> if cache_mgr:
        >>>     # Store with TTL
        >>>     cache_mgr.set('config:silver', data, ttl=3600)
        >>>     
        >>>     # Get with lazy loading
        >>>     data = cache_mgr.get('config:silver', loader=load_silver_config)
        >>>     
        >>>     # Clear by pattern
        >>>     cache_mgr.clear('stats:*')
    """
    
    def __init__(self):
        """Initialize the cache manager."""
        super().__init__()
        self._storage: Dict[str, Any] = {}
        self._ttl_storage: Dict[str, float] = {}
        self._access_count: Dict[str, int] = {}
        self._initialized = False
        
    def get_feature_name(self) -> str:
        """Return the feature flag name.
        
        Returns:
            'cache' - the feature flag for this manager
        """
        return 'cache'
        
    def is_available(self) -> bool:
        """Check if caching is enabled.
        
        Returns:
            True if configuration_cache_enabled is True in settings
        """
        return self.feature_settings.global_cache_enabled
        
    def initialize(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Initialize the cache manager.
        
        Args:
            config: Optional configuration for initialization
        """
        if self._initialized:
            return
            
        self._storage = {}
        self._ttl_storage = {}
        self._access_count = {}
        self._initialized = True
        
        logger.info("CacheManager initialized successfully")
        
        if config:
            logger.debug(f"CacheManager initialized with config: {config}")
        
    def get(self, key: str, loader: Optional[Callable[[], Any]] = None) -> Any:
        """Get value from cache or load it.
        
        Args:
            key: Cache key (use namespaces like 'config:' or 'stats:')
            loader: Optional function to load value if not cached
            
        Returns:
            Cached value, loaded value, or None
        """
        if not self._initialized:
            logger.warning("CacheManager not initialized")
            return loader() if loader else None
            
        # Check cache first
        if self._is_cached(key):
            self._access_count[key] = self._access_count.get(key, 0) + 1
            logger.debug(f"Cache hit for key: {key}")
            return self._storage[key]
            
        # Not in cache - load if loader provided
        if loader:
            logger.debug(f"Cache miss for key: {key}, loading...")
            try:
                value = loader()
                self.set(key, value)
                return value
            except Exception as e:
                logger.error(f"Failed to load value for key {key}: {e}")
                return None
                
        return None
        
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache with optional TTL.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (optional)
        """
        if not self._initialized:
            logger.warning("CacheManager not initialized")
            return
            
        self._storage[key] = value
        self._access_count[key] = 0
        
        if ttl:
            self._ttl_storage[key] = time.time() + ttl
            logger.debug(f"Cached key: {key} (TTL: {ttl}s)")
        elif key in self._ttl_storage:
            # Remove TTL if setting without TTL
            del self._ttl_storage[key]
            logger.debug(f"Cached key: {key} (no TTL)")
        else:
            logger.debug(f"Cached key: {key}")
            
    def exists(self, key: str) -> bool:
        """Check if key exists in cache.
        
        Args:
            key: Cache key to check
            
        Returns:
            True if key exists and is not expired, False otherwise
        """
        if not self._initialized:
            return False
            
        return self._is_cached(key)
        
    def delete(self, key: str) -> bool:
        """Remove key from cache.
        
        Args:
            key: Cache key to remove
            
        Returns:
            True if key was removed, False if key didn't exist
        """
        if not self._initialized:
            logger.warning("CacheManager not initialized")
            return False
            
        if key in self._storage:
            del self._storage[key]
            if key in self._ttl_storage:
                del self._ttl_storage[key]
            if key in self._access_count:
                del self._access_count[key]
            logger.debug(f"Deleted cache key: {key}")
            return True
        return False
        
    def clear(self, pattern: str = "*") -> int:
        """Clear keys matching pattern.
        
        Args:
            pattern: Glob pattern (e.g., 'stats:*', '*:bronze')
            
        Returns:
            Number of keys cleared
        """
        if not self._initialized:
            logger.warning("CacheManager not initialized")
            return 0
            
        if pattern == "*":
            # Clear everything
            count = len(self._storage)
            self._storage.clear()
            self._ttl_storage.clear()
            self._access_count.clear()
            logger.info(f"Cleared all {count} cache entries")
            return count
            
        # Clear by pattern
        keys_to_delete = [
            key for key in self._storage
            if fnmatch.fnmatch(key, pattern)
        ]
        
        for key in keys_to_delete:
            self.delete(key)
            
        logger.info(f"Cleared {len(keys_to_delete)} cache entries matching '{pattern}'")
        return len(keys_to_delete)
        
    def _is_cached(self, key: str) -> bool:
        """Check if key is in cache and not expired.
        
        Args:
            key: Cache key to check
            
        Returns:
            True if key exists and is not expired
        """
        if key not in self._storage:
            return False
            
        # Check TTL
        if key in self._ttl_storage:
            if time.time() >= self._ttl_storage[key]:
                # Expired - remove
                self.delete(key)
                return False
                
        return True
        
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics.
        
        Returns:
            Dictionary containing cache statistics
        """
        if not self._initialized:
            return {
                'enabled': self.is_available(),
                'initialized': False,
                'total_keys': 0
            }
            
        # Get top 10 most accessed keys
        top_accessed = sorted(
            self._access_count.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]
        
        return {
            'enabled': self.is_available(),
            'initialized': True,
            'total_keys': len(self._storage),
            'keys_with_ttl': len(self._ttl_storage),
            'top_accessed': top_accessed
        }
        
    def cleanup(self) -> None:
        """Cleanup resources when shutting down."""
        self._storage.clear()
        self._ttl_storage.clear()
        self._access_count.clear()
        self._initialized = False
        logger.debug("CacheManager cleaned up")


# Auto-register when module is imported
register_feature('cache', CacheManager)
logger.debug("CacheManager registered with feature registry")