"""Base class for feature managers (plugins).

This module provides the abstract base class for all feature managers,
which act as pluggable add-ons providing cross-cutting functionality
to any layer of the application.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from medalflow.settings.features import FeatureSettings




class FeatureManager(ABC):
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
    
    
    @property
    def feature_settings(self) -> 'FeatureSettings':
        """Get the current feature settings.
        
        Returns:
            FeatureSettings: The current feature settings instance
        """
        from medalflow.settings import get_settings
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
    def initialize(self, config: Optional[dict[str, Any]] = None) -> None:
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
    
    # B027: cleanup is an intentional optional hook, not part of the contract.
    # Marking it @abstractmethod would force every future manager to implement a
    # no-op, contradicting the documented "override only if you need to" design.
    def cleanup(self) -> None:  # noqa: B027
        """Cleanup resources when feature is disabled or app shuts down.
        
        Override this method if your manager needs to release resources,
        close connections, flush caches, etc. Default implementation
        does nothing.
        """
        pass
