"""Query Builder Factory.

This module provides a factory for creating platform-specific query builders
with automatic configuration from environment settings.

The factory pattern encapsulates all the complexity of creating query builders,
automatically fetching settings and extracting required configurations.
"""

from typing import TYPE_CHECKING, TypeVar, Union, cast

from core.constants.compute import ComputeType
from core.query_builder.base import BaseQueryBuilder
from core.query_builder.synapse.serverless_builder import (
    SynapseServerlessQueryBuilder
)

if TYPE_CHECKING:
    from core.settings.compute import ComputeSettings


class QueryBuilderFactory:
    """Factory for creating platform-specific query builders.
    
    This factory automatically configures query builders by fetching
    all required settings from the environment. No parameters needed!
    
    Benefits:
        - Zero configuration: Just call the method
        - Full encapsulation: All complexity hidden inside
        - Auto-configuration: Settings fetched automatically
        - Simple API: No parameters to understand
    
    Example:
        >>> # Just call - no parameters needed!
        >>> synapse = QueryBuilderFactory.create_synapse_builder()
        >>> auto = QueryBuilderFactory.create()  # Auto-detects platform
    """
    
    @staticmethod
    def create_synapse_builder() -> SynapseServerlessQueryBuilder:
        """Create a Synapse query builder auto-configured from environment.
        
        This method handles all the complex configuration required for Synapse,
        including external data sources, file formats, storage locations, and
        table prefixes - all automatically from environment settings.
        
        Returns:
            Fully configured SynapseServerlessQueryBuilder instance.
        
        Raises:
            ImportError: If settings module not available.
            AttributeError: If required settings not found.
        
        Example:
            >>> # Simple - no parameters!
            >>> builder = QueryBuilderFactory.create_synapse_builder()
            >>> # Builder is ready to use with all settings configured
        """
        from core.settings import get_settings
        
        settings = get_settings()
              
        return SynapseServerlessQueryBuilder(settings)
    
    @staticmethod
    def create() -> BaseQueryBuilder:
        """Create appropriate query builder based on active compute type.
        
        This method automatically detects the active compute platform
        from settings and creates the appropriate builder.
        
        Returns:
            Platform-specific query builder for the active compute type.
        
        Raises:
            ValueError: If active compute type is not supported.
        
        Example:
            >>> # Auto-detects platform and creates appropriate builder
            >>> builder = QueryBuilderFactory.create()
        """
        from core.settings import get_settings
        
        settings = get_settings()
        active_type = settings.compute.compute_type
        
        if active_type == ComputeType.SYNAPSE:
            return QueryBuilderFactory.create_synapse_builder()
        else:
            raise ValueError(
                f"Unsupported compute type: {active_type}. "
                f"Supported types: SYNAPSE"
            )


# Union type for all concrete builders
ConcreteQueryBuilder = SynapseServerlessQueryBuilder


def get_query_builder() -> ConcreteQueryBuilder:
    """Get a query builder auto-configured for the active platform.
    
    This convenience function returns the specific query builder type
    for the active compute platform, preserving all platform-specific methods.
    
    Returns:
        Platform-specific query builder (SynapseServerlessQueryBuilder or 
        based on active compute settings.
    
    Example:
        >>> from core.query_builder.factory import get_query_builder
        >>> 
        >>> builder = get_query_builder()
        >>> # If Synapse is active, IDE shows Synapse-specific methods
        >>> 
        >>> # Type narrowing with isinstance for platform-specific features:
        >>> if isinstance(builder, SynapseServerlessQueryBuilder):
        >>>     # IDE knows this is Synapse builder
        >>>     builder.build_is_external_table_query("bronze", "table")
    """
    return QueryBuilderFactory.create()  # Returns Union type


def get_synapse_query_builder() -> SynapseServerlessQueryBuilder:
    """Get a Synapse query builder with full type information.
    
    Use this when you know you're working with Synapse and want
    full IDE support for Synapse-specific methods.
    
    Returns:
        SynapseServerlessQueryBuilder instance.
        
    Example:
        >>> builder = get_synapse_query_builder()
        >>> # All Synapse-specific methods available in IDE
        >>> builder.build_is_external_table_query("bronze", "table")
    """
    return QueryBuilderFactory.create_synapse_builder()
