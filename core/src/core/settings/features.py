from typing import Dict

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from core.common.exceptions import feature_not_enabled_error
from .base import CTEBaseSettings


class FeatureSettings(CTEBaseSettings):    
    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_prefix=''
    )
    
    cte_stats_enabled: bool = Field(
        default=True,
        description="Enable automatic statistics collection for query optimization. "
                   "Statistics improve query performance by helping the optimizer "
                   "choose efficient execution plans. Disable only if you have "
                   "alternative statistics management."
    )
    
    snapshots_enabled: bool = Field(
        default=False,
        description="Enable the snapshot layer for historical data tracking. "
                   "When enabled, the system maintains point-in-time copies "
                   "of data for audit trails and historical analysis. "
                   "Key reshuffling is automatically enabled to ensure consistent keys. "
                   "Requires additional storage and processing time."
    )

    global_cache_enabled: bool = Field(
        default=True,
        description="Enable caching for configuration data loaded from the data lake. "
                   "Caching improves performance by reducing repeated data fetches. "
                   "Disable only if you have specific reasons to always fetch fresh data."
    )
    
    @property
    def key_reshuffle_enabled(self) -> bool:
        """Check if key reshuffle is enabled.
        
        Key reshuffle is automatically enabled when snapshots are enabled
        to ensure consistent surrogate keys across snapshot operations.
        
        Returns:
            bool: True if snapshots (and thus key reshuffle) are enabled
        """
        return self.snapshots_enabled
        
    def _get_feature_list(self) -> list[str]:
        """Get a list of all feature field names.
        
        Returns:
            List[str]: All feature field names (with '_enabled' suffix)
        """
        return [ field_name for field_name in self.__class__.model_fields.keys() if field_name.endswith('_enabled') ]

    def get_enabled_features(self) -> list[str]:
        """Get a list of all enabled features.
        
        Provides a list of currently active feature keys (without '_enabled' suffix)
        for programmatic use, logging, or debugging purposes.
        
        Returns:
            List[str]: Enabled feature keys (e.g., 'cte_stats', 'snapshots')
            
        Example:
            >>> settings = get_settings()
            >>> features = settings.features.get_enabled_features()
            >>> print(f"Active features: {', '.join(features)}")
            >>> # Output: "Active features: cte_stats, synapse_link"
        """
        enabled = [ feature.replace('_enabled', '') for feature in self._get_feature_list() if getattr(self, feature) ]
                
        return enabled
    
    def check_feature(self, feature_name: str, raise_on_disabled: bool = True) -> bool:
        """Check if a feature is enabled, optionally raising an error if disabled.
        
        This method provides centralized feature checking with optional access control.
        When a feature is disabled and raise_on_disabled is True, it raises a
        FeatureNotEnabled exception with helpful guidance.
        
        Args:
            feature_name: Name of the feature to check (without '_enabled' suffix)
            raise_on_disabled: If True, raises FeatureNotEnabled when feature is disabled
            
        Returns:
            bool: True if feature is enabled, False otherwise (only when raise_on_disabled=False)
            
        Raises:
            FeatureNotEnabled: If feature is disabled and raise_on_disabled=True
            ValueError: If feature_name is not recognized
            
        Example:
            >>> features = get_settings().features
            >>> 
            >>> # Check with exception on disabled (default)
            >>> features.check_feature('snapshots')  # Raises if disabled
            >>> 
            >>> # Check without exception
            >>> if features.check_feature('snapshots', raise_on_disabled=False):
            >>>     # Process snapshots
            >>>     pass
        """
        feature_map = { feature.replace('_enabled', ''): getattr(self, feature) for feature in self._get_feature_list() }
        guidance = { feature.replace('_enabled', ''): f"Set {feature.upper()}=true to enable {feature.replace('_enabled', '').replace('_', ' ')}."  for feature in self._get_feature_list() }

        
        if feature_name not in feature_map.keys():
            available_features = ', '.join(sorted(feature_map.keys()))
            raise ValueError(
                f"Unknown feature '{feature_name}'. "
                f"Available features: {available_features}"
            )
        
        is_enabled = feature_map[feature_name]
        
        if not is_enabled and raise_on_disabled:
            raise feature_not_enabled_error(
                feature_name=feature_name,
                message=guidance.get(feature_name, "")
            )
        
        return is_enabled
    
    def get_feature_status(self) -> Dict[str, bool]:
        """Get the status of all features.
        
        Returns a dictionary mapping feature names to their enabled status.
        Useful for debugging, logging, or displaying configuration.
        
        Returns:
            Dict[str, bool]: Feature names mapped to enabled status
            
        Example:
            >>> features = get_settings().features
            >>> status = features.get_feature_status()
            >>> for feature, enabled in status.items():
            >>>     print(f"{feature}: {'✓' if enabled else '✗'}")
        """
        return {feature.replace('_enabled', ''): getattr(self, feature) for feature in self._get_feature_list() }