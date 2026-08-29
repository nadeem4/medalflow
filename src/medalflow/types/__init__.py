"""Type definitions for MedalFlow.

This module provides base types used throughout the MedalFlow framework,
including metadata classes for different medallion layers and query operations.
"""

from .base import CTEBaseModel
from .metadata import (
    # Layer metadata
    BronzeMetadata,
    ClassMetadata,
    DiscoveredMethod,
    GoldMetadata,
    QueryAnalysis,
    # Query metadata
    QueryMetadata,
    SilverMetadata,
    SQLDependencies,
    TransformationMetadata,
)
from .sql import RawSQL, SQLFragment

__all__ = [
    # Base model
    "CTEBaseModel",
    # SQL markers
    "RawSQL",
    "SQLFragment",
    # Layer metadata
    "BronzeMetadata",
    "SilverMetadata",
    "GoldMetadata",
    "TransformationMetadata",
    "ClassMetadata",
    # Query metadata
    "QueryMetadata",
    "DiscoveredMethod",
    "SQLDependencies",
    "QueryAnalysis",
]
