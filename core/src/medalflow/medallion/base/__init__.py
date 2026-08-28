"""Base components for the medallion architecture.

This module provides the foundational classes and decorators used across all
medallion layers. These base components establish the common patterns and
interfaces that layer-specific implementations build upon.

Components:
    - _BaseSequencer: Internal abstract base class for all sequencers
    - query_metadata: Decorator for annotating query methods
    - QueryMetadata: Type definition for query metadata
"""

from medalflow.types.metadata import QueryMetadata

from .decorators import query_metadata
from .sequencer import _BaseSequencer

__all__ = [
    "_BaseSequencer",
    "query_metadata",
    "QueryMetadata",
]
