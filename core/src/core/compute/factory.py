"""Platform factory for compute operations.

This module provides factory classes for creating compute platform instances
and engines. It supports multiple platforms (Synapse, Fabric) and implements
the factory pattern for easy extensibility.

Overview:
    The PlatformFactory is the central entry point for creating platform instances
    in the compute module. It manages platform registration, instantiation, and
    configuration using a registry-based factory pattern. This design allows for
    easy extension with new platforms without modifying existing code.

Design Pattern:
    The factory implements the Factory Method pattern with a registry:
    
    1. **Registration**: Platforms are registered with string identifiers
    2. **Creation**: Platforms are created using identifiers and settings
    3. **Configuration**: Settings are automatically loaded based on platform type
    4. **Validation**: Type checking ensures platform compatibility

Platform Support:
    The factory supports built-in platforms only:
    - Synapse: Azure Synapse Analytics
    - Fabric: Microsoft Fabric

Configuration:
    The factory automatically loads platform-specific settings from the
    global settings object based on the platform type:
    
    - Synapse: settings.compute.synapse
    - Fabric: settings.compute.fabric
    - Custom: Must be added to settings structure

Thread Safety:
    The factory uses class-level state for the platform registry. While
    registration is not thread-safe, platform creation is safe for concurrent
    use after initial setup.
"""

from typing import Any, Dict, Type, Optional

from typing import TYPE_CHECKING

from core.constants.compute import ComputeEnvironment
from core.compute.platforms.base import _BasePlatform
from core.compute.platforms.synapse import SynapsePlatform
from core.compute.platforms.fabric import FabricPlatform
from core.logging import get_logger
from core.constants import ComputeType


logger = get_logger(__name__)


class _PlatformFactory:
    """Internal implementation detail. Do not use directly.
    
    This class is not part of the public API and may change without notice.
    
    Factory for creating compute platform instances.
    
    This factory manages platform registration and creation,
    allowing easy extension with new platforms.
    
    The PlatformFactory maintains a registry of available platform implementations
    and provides methods to create configured instances. It serves as the main
    entry point for platform abstraction in the compute module.
    
    Attributes:
        _platforms (Dict[str, Type[BasePlatform]]): Registry mapping platform
            identifiers to their implementation classes. Pre-populated with
            built-in platforms (synapse, fabric).
    
    Class Methods:
        register_platform: Add a new platform type to the registry
        create_platform: Create a platform instance with configuration
        get_available_platforms: List all registered platform identifiers
        unregister_platform: Remove a platform from the registry
    
    Example:
        >>> # Using default settings
        >>> platform = PlatformFactory.create_platform()
        >>> 
        >>> # Specifying platform and environment
        >>> platform = PlatformFactory.create_platform(
        ...     name="fabric",
        ...     environment=ComputeEnvironment.CONSUMPTION
        ... )
        >>> 
        >>> # Only built-in platforms are supported
        >>> platform = PlatformFactory.create_platform("synapse")
        >>> platform = PlatformFactory.create_platform("fabric")
    
    Note:
        Platform creation requires corresponding settings to be configured
        in the application settings. For built-in platforms, these are:
        - settings.compute.synapse for Synapse
        - settings.compute.fabric for Fabric
    """
    
    
    
    @classmethod
    def create(cls,  environment: ComputeEnvironment = ComputeEnvironment.ETL) -> _BasePlatform:
        """Create a platform instance using settings.
        
        This is the main factory method for creating platform instances. It handles:
        - Platform selection (from parameter or settings)
        - Settings retrieval for the selected platform
        - Instance creation with proper configuration
        
        Args:
            name: Platform name (if None, uses settings.compute.compute_type)
            environment: Compute environment (ETL or CONSUMPTION)
            settings: Optional settings object. If None, will import and use get_settings()
            
        Returns:
            BasePlatform: Configured platform instance ready for use
            
        Raises:
            PlatformNotSupportedError: If platform not registered
            ValueError: If no settings configured for the platform
            
        Example:
            >>> # Create platform using settings
            >>> platform = PlatformFactory.create_platform()
            >>> 
            >>> # Create specific platform
            >>> platform = PlatformFactory.create_platform("fabric")
            >>> 
            >>> # Create for consumption workload
            >>> platform = PlatformFactory.create_platform(
            ...     environment=ComputeEnvironment.CONSUMPTION
            ... )
        """
   
        from core.settings import get_settings
        settings = get_settings()
        
        platform_type = settings.compute.compute_type
        
        
        if platform_type == ComputeType.SYNAPSE:
            platform_settings = settings.compute.synapse
            platform = SynapsePlatform(platform_settings, environment)
        elif platform_type == ComputeType.FABRIC:
            platform_settings = settings.compute.fabric
            platform = FabricPlatform(platform_settings, environment)
        else:
            raise ValueError(f"No settings configured for platform: {platform_type}")
        
        logger.info(
            "Created platform",
            platform=platform_type.value,
            environment=environment.value
        )
        return platform

    


def get_platform_factory() -> Type[_PlatformFactory]:
    """Get the platform factory class.
    
    This function provides access to the platform factory class. It is
    primarily intended for internal use and testing. External code should
    use PlatformFactory directly.
    
    Returns:
        Type[PlatformFactory]: The platform factory class
        
    Example:
        >>> factory_class = get_platform_factory()
        >>> platform = factory_class.create_platform("synapse")
    """
    return _PlatformFactory