"""Silver layer components for cleansed, conformed data.

The Silver layer transforms raw Bronze data into cleansed, conformed tables.
Transformation SQL is declared on sequencer methods with ``@query_metadata``;
dependencies between queries are derived from the SQL itself, so no explicit
ordering is declared.

Components:
    - SilverTransformationSequencer: Sequencer for Silver layer transformations
    - silver_metadata: Class decorator for Silver sequencer configuration
    - SilverMetadata: Model backing the ``@silver_metadata`` configuration
"""

from medalflow.types.metadata import SilverMetadata

from .decorators import silver_metadata
from .sequencer import SilverTransformationSequencer

__all__ = [
    "SilverTransformationSequencer",
    "silver_metadata",
    "SilverMetadata",
]
