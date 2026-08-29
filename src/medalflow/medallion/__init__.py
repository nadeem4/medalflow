"""The three layers, and the decorators that declare a model in each.

Bronze ingests a source table as it stands, silver transforms, gold
publishes. Each layer is two things: a decorator marking a class as one of
its models (``bronze_metadata``, ``silver_metadata``, ``gold_metadata``) and
a sequencer base class turning that model into operations. Inside a model,
``query_metadata`` marks the methods that return SQL.

Ordering does not live in the layers. A layer knows how to build its own
tables; :func:`medalflow.compile` walks all three and orders them into one
plan, using the dependencies read out of the SQL itself.

This is also where bronze is reached from -- unlike silver and gold, it is
not re-exported from :mod:`medalflow`.
"""

# Import base components
from medalflow.constants.compute import EngineType
from medalflow.constants.medallion import ExecutionMode, Layer

# Import Enums from constants
from medalflow.constants.sql import QueryType

from .base.decorators import query_metadata
from .bronze.decorators import bronze_metadata

# Import Bronze layer components
from .bronze.sequencer import BronzeSequencer
from .gold.decorators import gold_metadata, view_metadata

# Import Gold layer components
from .gold.sequencer import GoldSequencer
from .orchestration.execution_orchestrator import ExecutionPlanOrchestrator
from .silver.decorators import silver_metadata

# Import Silver layer components
from .silver.sequencer import SilverTransformationSequencer
from .types import ExecutionPlan

__all__ = [
    "SilverTransformationSequencer",
    "GoldSequencer",
    "BronzeSequencer",
    # Metadata decorators (public API)
    "bronze_metadata",
    "silver_metadata",
    "gold_metadata",
    "view_metadata",  # Alias for gold_metadata
    "query_metadata",
    "QueryType",
    "ExecutionMode",
    "Layer",
    "EngineType",
    "ExecutionPlanOrchestrator",
    "ExecutionPlan",
]
