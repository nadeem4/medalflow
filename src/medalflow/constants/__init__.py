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
from medalflow.constants.compute import (
    ComputeEnvironment,
    ComputeType,
    EngineType,
)

# Data Lake constants
from medalflow.constants.datalake import (
    DataLakeAuthMethod,
    LakeType,
)

# Medallion constants
from medalflow.constants.medallion import CalendarType, ExecutionMode, Layer, SnapshotFrequency

# SQL/Query constants
from medalflow.constants.sql import QueryType

# Validation constants
from medalflow.constants.validation import (
    ValidationLevel,
)

__all__ = [
    # Compute
    "ComputeType",
    "ComputeEnvironment",
    "EngineType",
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
]
