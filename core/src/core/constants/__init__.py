"""Constants module for MedalFlow.

This module contains all constant values and enumerations used throughout
the MedalFlow framework. As Layer 0 in the architecture, this module has
no dependencies on other MedalFlow modules.

Organization:
    - compute: Compute platform and engine constants
    - medallion: Medallion architecture layer constants
    - datalake: Data lake configuration constants
    - validation: Data validation level constants
    - dataframe: DataFrame processing engine constants
"""

# Compute constants
from core.constants.compute import (
    ComputeType,
    ComputeEnvironment,
    EngineType,
    JobStatus,
)

# SQL/Query constants
from core.constants.sql import QueryType

# Medallion constants  
from core.constants.medallion import (
    ExecutionMode,
    Layer,
    SnapshotFrequency,
    CalendarType
)

# Data Lake constants
from core.constants.datalake import (
    LakeType,
    DataLakeAuthMethod,
)

# Validation constants
from core.constants.validation import (
    ValidationLevel,
)


from .core import LayerType

__all__ = [
    # Compute
    "ComputeType",
    "ComputeEnvironment", 
    "EngineType",
    "JobStatus",
    # Medallion
    "QueryType",
    "ExecutionMode",
    "Layer",
    "SnapshotFrequency",
    "CalendarType",
    # Data Lake
    "LakeType",
    "DataLakeAuthMethod",
    # Validation
    "ValidationLevel",
    "LayerType"
]