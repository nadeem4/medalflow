"""Bronze layer components for raw data ingestion.

The Bronze layer is responsible for ingesting raw data from source systems
with minimal transformation. This layer focuses on data extraction and
initial validation while preserving the original data format.

Components:
    - BronzeSequencer: One declared Bronze model, which is one Bronze table
    - IntrospectedBronzeSequencer: The opt-in alternative, deriving tables from
      a live INFORMATION_SCHEMA query
    - bronze_metadata: Decorator for Bronze layer class configuration
"""

from .decorators import bronze_metadata
from .sequencer import BronzeSequencer, IntrospectedBronzeSequencer

__all__ = [
    "BronzeSequencer",
    "IntrospectedBronzeSequencer",
    "bronze_metadata",
]
