"""Bronze layer components for raw data ingestion.

The Bronze layer is responsible for ingesting raw data from source systems
with minimal transformation. This layer focuses on data extraction and
initial validation while preserving the original data format.

Components:
    - BronzeSequencer: Sequencer for Bronze layer ETL processes
    - bronze_metadata: Decorator for Bronze layer class configuration
"""

from .sequencer import BronzeSequencer
from .decorators import bronze_metadata

__all__ = [
    "BronzeSequencer",
    "bronze_metadata",
]