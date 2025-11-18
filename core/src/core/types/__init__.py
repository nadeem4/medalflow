"""Type definitions for MedalFlow.

This module provides base types used throughout the MedalFlow framework,
including metadata classes for different medallion layers and query operations.
"""

from .base import CTEBaseModel
from .metadata import (
    # Layer metadata
    BronzeMetadata,
    SilverMetadata,
    GoldMetadata,
    SnapshotMetadata,
    TransformationMetadata,
    ClassMetadata,
    # Query metadata
    QueryMetadata,
    DiscoveredMethod,
    SQLDependencies,
    QueryAnalysis,
)

__all__ = [
    # Base model
    'CTEBaseModel',
    # Layer metadata
    'BronzeMetadata',
    'SilverMetadata',
    'GoldMetadata',
    'SnapshotMetadata',
    'TransformationMetadata',
    'ClassMetadata',
    # Query metadata
    'QueryMetadata',
    'DiscoveredMethod',
    'SQLDependencies',
    'QueryAnalysis',
]