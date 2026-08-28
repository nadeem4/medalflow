"""Feature management system for pluggable add-ons.

This package provides a plugin architecture for cross-cutting features
that can be used by any layer of the application (compute, datalake,
medallion, etc.).

The feature management system consists of:
- FeatureManager: Base class for all feature plugins
- FeatureRegistry: Central registry managing feature lifecycle
- Feature Managers: Individual plugins (stats, cache, telemetry, etc.)

Example:
    >>> from medalflow.core.features import get_feature_manager
    >>> 
    >>> # Get a feature manager (returns None if disabled)
    >>> stats_mgr = get_feature_manager('stats')
    >>> if stats_mgr:
    >>>     if stats_mgr.should_create_stats('MyTable', 'bronze'):
    >>>         columns = stats_mgr.get_stats_columns('MyTable', 'bronze')
    >>>         # Use the stats information
    
    Using type annotations for IDE support:
    >>> from medalflow.core.features import get_feature_manager
    >>> from medalflow.protocols.features import CacheProtocol
    >>> 
    >>> # Use type annotation for IDE autocomplete
    >>> cache: Optional[CacheProtocol] = get_feature_manager('cache')
    >>> if cache:
    >>>     cache.set('key', 'value')  # IDE knows all methods!
"""

# Re-export protocols for convenience
from medalflow.protocols.features import (
    CacheProtocol,
    StatsProtocol,
)

from .base import FeatureManager
from .registry import (
    get_available_features,
    get_feature_manager,
    register_feature,
)

__all__ = [
    # Base class
    'FeatureManager',
    
    # Main functions
    'get_feature_manager',
    'get_available_features',
    'register_feature',
    
    # Protocols (for type annotations)
    'CacheProtocol',
    'StatsProtocol',
]