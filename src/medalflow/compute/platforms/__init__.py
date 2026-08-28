"""Platform abstractions for compute support.

This module provides the platform abstraction layer that lets MedalFlow run
operations without the medallion layer knowing which compute service executes
them. Azure Synapse Analytics is the only platform currently implemented.

Platforms serve as the central orchestration point for:
    - Engine management (SQL)
    - Query builder selection
    - Engine selection for an operation
    - Connection testing and validation

Architecture:
    - _BasePlatform: Abstract base class defining the platform interface
    - _SynapsePlatform: the Synapse implementation
    - Each platform builds its own engine and query builder in
      ``_initialize_dependencies``

Creating a platform:
    Platforms are built by ``medalflow.compute.create_platform()``, which reads
    ``settings.compute.compute_type``.

Example:
    from medalflow.compute import create_platform
    from medalflow.operations import CreateTable

    platform = create_platform()

    result = platform.execute_operation(
        CreateTable(
            schema_name="silver",
            object_name="customers",
            select_query="SELECT * FROM bronze.raw_customers",
        )
    )
    print(f"Succeeded: {result.success}")
    print(f"Engine used: {result.engine_used}")
"""

from .base import _BasePlatform
from .synapse import SynapsePlatform as _SynapsePlatform

__all__ = ["_BasePlatform", "_SynapsePlatform"]
