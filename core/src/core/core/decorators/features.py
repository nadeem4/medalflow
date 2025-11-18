"""Feature management decorator.

This module provides the feature_gate decorator for ensuring
FeatureManager subclasses only instantiate when their feature is available.
"""

import functools
from typing import Type, TypeVar

from core.common.exceptions import feature_not_enabled_error
from core.protocols.features import FeatureManagerProtocol

T = TypeVar('T', bound=FeatureManagerProtocol)


def feature_gate(cls: Type[T]) -> Type[T]:
    """Decorator ensuring FeatureManager only instantiates when available.
    
    This decorator ensures that feature manager classes are only instantiated
    when their feature is available/enabled by checking their is_available() method.
    The decorator preserves the exact type of the class it decorates.
    
    Args:
        cls: A class implementing FeatureManagerProtocol
        
    Returns:
        The same class type with instantiation protection
    
    Example:
        >>> @feature_gate
        >>> class StatsManager(FeatureManager):
        >>>     def get_feature_name(self) -> str:
        >>>         return 'cte_stats'
        >>>     
        >>>     def custom_stats_method(self) -> None:
        >>>         # Subclass-specific method
        >>>         pass
        >>> 
        >>> # Type checker knows this is StatsManager, not just FeatureManagerProtocol
        >>> manager = StatsManager()  # Raises feature_not_enabled_error if disabled
        >>> manager.custom_stats_method()  # Type-safe access to subclass methods
    
    Raises:
        CTEError: If the feature is not available when trying to instantiate (error_code=FEATURE_DISABLED)
    """
    original_init = cls.__init__
    
    @functools.wraps(original_init)
    def new_init(self: T, *args, **kwargs):
        original_init(self, *args, **kwargs)
        if not self.is_available():
            raise feature_not_enabled_error(
                feature_name=self.get_feature_name(),
                message=f"Feature '{self.get_feature_name()}' is not available"
            )
    
    cls.__init__ = new_init
    return cls