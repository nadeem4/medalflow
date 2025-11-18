"""Feature registry for managing feature plugins.

This module provides a central registry for all feature managers,
managing their lifecycle and providing unified access across the application.
"""

from typing import Dict, Optional, Type, List, Any
import logging

from .base import FeatureManager
from core.common.exceptions import CTEError, ErrorCode


logger = logging.getLogger(__name__)


class FeatureRegistry:
    """Central registry for all feature managers (plugins).
    
    This registry manages the lifecycle of feature plugins and
    provides a unified interface for accessing them. It handles:
    - Registration of feature managers
    - Lazy instantiation based on feature flags
    - Caching of manager instances
    - Auto-discovery of available managers
    
    Example:
        >>> # Register a feature manager
        >>> registry = FeatureRegistry()
        >>> registry.register('my_feature', MyFeatureManager)
        >>> 
        >>> # Get a manager (returns None if feature disabled)
        >>> manager = registry.get_manager('my_feature')
        >>> if manager:
        >>>     manager.do_something()
    """
    
    def __init__(self):
        """Initialize the registry."""
        self._managers: Dict[str, Type[FeatureManager]] = {}
        self._instances: Dict[str, Optional[FeatureManager]] = {}
        self._initialized: bool = False
    
    def register(self, feature_name: str, manager_class: Type[FeatureManager]) -> None:
        """Register a feature manager.
        
        Once registered, subsequent registration attempts are ignored to maintain
        consistency and prevent accidental overrides.
        
        Args:
            feature_name: Name of the feature (e.g., 'stats', 'cache')
            manager_class: The FeatureManager subclass to register
        """
        if feature_name in self._managers:
            logger.debug(
                f"Feature '{feature_name}' already registered, ignoring re-registration attempt"
            )
            return
        
        self._managers[feature_name] = manager_class
        logger.debug(f"Registered feature manager: {feature_name} -> {manager_class.__name__}")
    
    def get_manager(self, feature_name: str) -> Optional[FeatureManager]:
        """Get a feature manager instance.
        
        Returns None if the feature is disabled. Managers are lazily
        instantiated on first access and cached for subsequent calls.
        
        Args:
            feature_name: Name of the feature to get
            
        Returns:
            FeatureManager instance if feature is enabled, None otherwise
            
        Raises:
            ValueError: If feature_name is not registered
        """
        if feature_name not in self._managers:
            available = ', '.join(self._managers.keys()) if self._managers else 'none'
            raise ValueError(
                f"Unknown feature: '{feature_name}'. Available features: {available}"
            )
        
        # Check if already instantiated
        if feature_name not in self._instances:
            manager_class = self._managers[feature_name]
            
            try:
                # Create instance (singleton)
                manager = manager_class()
                
                logger.info(f"Initializing feature manager: {feature_name}")
                manager.initialize()
                self._instances[feature_name] = manager
                
            except CTEError as e:
                if e.error_code == ErrorCode.FEATURE_DISABLED:
                    # This is expected when feature is disabled - not an error
                    logger.debug(f"Feature '{feature_name}' is disabled")
                    self._instances[feature_name] = None
                else:
                    # Re-raise other medalflow errors
                    raise
                
            except Exception as e:
                # This is an actual error
                logger.error(f"Failed to initialize feature manager '{feature_name}': {e}")
                self._instances[feature_name] = None
        
        return self._instances[feature_name]
    
    def get_available_features(self) -> List[str]:
        """Get list of all available (enabled) features.
        
        Returns:
            List of feature names that are currently enabled
        """
        available = []
        for name in self._managers:
            manager = self.get_manager(name)
            if manager is not None:
                available.append(name)
        return available
    
    def get_all_features(self) -> List[str]:
        """Get list of all registered features (enabled or disabled).
        
        Returns:
            List of all registered feature names
        """
        return list(self._managers.keys())
    
    def is_feature_available(self, feature_name: str) -> bool:
        """Check if a feature is available (enabled).
        
        Args:
            feature_name: Name of the feature to check
            
        Returns:
            True if feature is registered and enabled, False otherwise
        """
        try:
            manager = self.get_manager(feature_name)
            return manager is not None
        except ValueError:
            return False
    
    def auto_discover(self) -> None:
        """Auto-discover and register all feature managers.
        
        This method imports all manager modules, which triggers
        their auto-registration. It's idempotent - calling it
        multiple times has no effect after the first call.
        """
        if self._initialized:
            return
        
        logger.debug("Auto-discovering feature managers...")
        
        try:
            # Import all managers to trigger registration
            # Each manager module should register itself when imported
            from . import managers  # This imports all managers via __init__.py
            # Managers auto-register when imported
            
            self._initialized = True
            if self._managers:
                logger.info(f"Auto-discovered features: {', '.join(self._managers.keys())}")
        except ImportError as e:
            logger.warning(f"Failed to auto-discover some features: {e}")
            self._initialized = True
    
    def reset(self) -> None:
        """Reset the registry (mainly for testing).
        
        This clears all registrations and instances. Useful for
        testing scenarios where you need a clean state.
        """
        # Cleanup all instances
        for instance in self._instances.values():
            if instance:
                instance.cleanup()
        
        self._managers.clear()
        self._instances.clear()
        self._initialized = False
        logger.debug("Feature registry reset")


# Global registry instance
_global_registry = FeatureRegistry()


def get_feature_manager(feature_name: str) -> Optional[FeatureManager]:
    """Get a feature manager from the global registry.
    
    This is the main entry point for accessing feature managers
    throughout the application. It ensures auto-discovery has run
    and returns the requested manager if available.
    
    Args:
        feature_name: Name of the feature (e.g., 'stats', 'cache')
        
    Returns:
        FeatureManager instance if feature is enabled, None otherwise
        
    Example:
        >>> stats_manager = get_feature_manager('stats')
        >>> if stats_manager:
        >>>     stats_manager.do_something()
    """
    _global_registry.auto_discover()
    return _global_registry.get_manager(feature_name)


def get_available_features() -> List[str]:
    """Get list of all available (enabled) features.
    
    Returns:
        List of feature names that are currently enabled
    """
    _global_registry.auto_discover()
    return _global_registry.get_available_features()


def register_feature(feature_name: str, manager_class: Type[FeatureManager]) -> None:
    """Register a feature manager with the global registry.
    
    This is typically called automatically by manager modules
    when they are imported.
    
    Args:
        feature_name: Name of the feature
        manager_class: The FeatureManager subclass
    """
    _global_registry.register(feature_name, manager_class)