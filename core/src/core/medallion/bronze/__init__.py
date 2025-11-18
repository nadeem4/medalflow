"""Bronze layer components for raw data ingestion.

The Bronze layer is responsible for ingesting raw data from source systems
with minimal transformation. This layer focuses on data extraction and
initial validation while preserving the original data format.

Components:
    - BronzeSequencer: Sequencer for Bronze layer ETL processes
    - BronzeProcessor: Processor for Bronze layer operations
    - BronzeValidator: Validator for Bronze layer data quality
    - bronze_metadata: Decorator for Bronze layer class configuration
"""

from .sequencer import BronzeSequencer
from .processor import _BronzeProcessor as BronzeProcessor
from .decorators import bronze_metadata
from .validator import _BronzeValidator as BronzeValidator

__all__ = [
    "BronzeSequencer",
    "BronzeProcessor",
    "bronze_metadata",
    "BronzeValidator",
]