"""Bronze layer components for raw data ingestion.

The Bronze layer is responsible for ingesting raw data from source systems
with minimal transformation. This layer focuses on data extraction and
initial validation while preserving the original data format.

Components:
    - BronzeSequencer: One declared Bronze model, which is one Bronze table
    - BronzeMetadataDiscovery: The declared models in the bronze package
    - IntrospectedBronzeDiscovery: The opt-in alternative, deriving one model
      per table from a live INFORMATION_SCHEMA query
    - bronze_metadata: Decorator for Bronze layer class configuration
"""

from .decorators import bronze_metadata
from .sequencer import BronzeSequencer

__all__ = [
    "BronzeSequencer",
    "bronze_metadata",
]
