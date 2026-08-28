"""Platform abstractions for multi-engine compute support.

This module provides the platform abstraction layer that enables MedalFlow to work
seamlessly across different compute platforms like Azure Synapse Analytics and
Azure Synapse.

Platforms serve as the central orchestration point for:
    - Engine management (SQL)
    - Query builder selection
    - Engine selection logic (AUTO mode)
    - Connection testing and validation

Architecture:
    - BasePlatform: Abstract base class defining the platform interface
    - Platform implementations: Synapse (and extensible for others)
    - Each platform manages its own engines and query builders

The Factory Pattern:
    Platforms are created through PlatformFactory, which uses settings to
    configure the appropriate platform instance.

Key Features:
    - Lazy loading of engines (created only when needed)
    - Automatic engine selection based on query context
    - Platform-specific optimizations
    - Unified interface across different platforms

Example:
    from core.compute import PlatformFactory
    from core.operations import QueryContext
    
    # Create platform from settings
    platform = PlatformFactory.create_platform("synapse")
    
    # Platform automatically selects best engine
    context = QueryContext(
        preferred_engine=EngineType.AUTO,
        estimated_rows=50_000_000,
        has_complex_transformations=True
    )
    engine_type = platform.select_engine(context)  # Returns SPARK
    
    # Get platform info
    info = platform.get_info()
    print(f"Platform: {info['name']}")
    print(f"Supported engines: {info['supported_engines']}")
"""

from .base import _BasePlatform
from .synapse import SynapsePlatform as _SynapsePlatform

__all__ = ["_BasePlatform", "_SynapsePlatform"]