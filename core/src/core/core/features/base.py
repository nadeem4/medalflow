"""Base class for feature managers (plugins).

This module provides the abstract base class for all feature managers,
which act as pluggable add-ons providing cross-cutting functionality
to any layer of the application.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, ClassVar, TYPE_CHECKING

from core.protocols.features import FeatureManagerProtocol

if TYPE_CHECKING:
    from core.settings.features import FeatureSettings




class FeatureManager(FeatureManagerProtocol, ABC):
    """Base class for all feature managers (plugins).
    
    Feature managers are pluggable add-ons that provide
    cross-cutting functionality to any layer of the application.
    They follow a singleton pattern per manager type and can be
    enabled/disabled based on feature flags.
    
    Subclasses must implement:
        - get_feature_name(): Return the feature flag name
        - initialize(): Setup the manager with optional config
        - is_available(): Check if feature is enabled
    
    Example:
        >>> class MyFeatureManager(FeatureManager):
        >>>     def get_feature_name(self) -> str:
        >>>         return 'my_feature'
        >>>     
        >>>     def is_available(self) -> bool:
        >>>         settings = get_settings()
        >>>         return settings.features.my_feature_enabled
        >>>     
        >>>     def initialize(self, config=None):
        >>>         # Setup resources
        >>>         pass
    """
    
    _instances: ClassVar[Dict[type, 'FeatureManager']] = {}
    
    def __new__(cls):
        """Singleton pattern - one instance per manager type.
        
        This ensures that each feature manager type has only one
        instance throughout the application lifecycle.
        """
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls)
        return cls._instances[cls]
    
    @property
    def feature_settings(self) -> 'FeatureSettings':
        """Get the current feature settings.
        
        Returns:
            FeatureSettings: The current feature settings instance
        """
        from core.settings import get_settings
        settings = get_settings()
        return settings.features
        
    
    
    @abstractmethod
    def get_feature_name(self) -> str:
        """Return the feature flag name that controls this manager.
        
        This should match the feature flag name in FeatureSettings.
        
        Returns:
            str: Feature name (e.g., 'cte_stats', 'snapshots', 'telemetry')
        """
        pass
    
    @abstractmethod
    def initialize(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Initialize the manager with optional configuration.
        
        This method is called once when the manager is first accessed
        and the feature is enabled. Use it to setup resources, load
        configuration, initialize caches, etc.
        
        Args:
            config: Optional configuration dictionary for the manager
        """
        pass
    
    def is_available(self) -> bool:
        """Check if this feature is currently available/enabled.
        
        This method should check the corresponding feature flag
        in settings to determine if the feature is enabled.
        
        Returns:
            bool: True if feature is enabled, False otherwise
        """
        feature_name = f'{self.get_feature_name()}_enabled'
        return getattr(self.feature_settings, feature_name, False)
    
    def cleanup(self) -> None:
        """Cleanup resources when feature is disabled or app shuts down.
        
        Override this method if your manager needs to release resources,
        close connections, flush caches, etc. Default implementation
        does nothing.
        """
        pass
    
    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (mainly for testing).
        
        This method clears the cached instance, forcing a new instance
        to be created on next access. Useful for testing scenarios.
        """
        if cls in cls._instances:
            # Call cleanup before removing
            instance = cls._instances[cls]
            instance.cleanup()
            del cls._instances[cls]