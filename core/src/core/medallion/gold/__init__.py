"""Gold layer components for business-ready analytics.

The Gold layer creates business-ready aggregated views and analytics.
This layer provides optimized, denormalized data structures for reporting,
dashboards, and analytical workloads.

Components:
    - GoldSequencer: Sequencer for Gold layer view creation
    - GoldProcessor: Processor for Gold layer operations
    - GoldValidator: Validator for Gold layer data quality
    - gold_metadata: Decorator for Gold layer class configuration
    - view_metadata: Alias for gold_metadata (backward compatibility)
"""

from .sequencer import GoldSequencer
from .decorators import gold_metadata, view_metadata
from .processor import _GoldProcessor as GoldProcessor
from .validator import _GoldValidator as GoldValidator

__all__ = [
    "GoldSequencer",
    "GoldProcessor",
    "GoldValidator",
    "gold_metadata",
    "view_metadata",
]