"""Validator for Snapshot layer operations.

This module provides validation functionality specific to the Snapshot layer
of the medallion architecture.
"""

from ..base.validator import _BaseValidator


class _SnapshotValidator(_BaseValidator):
    """Internal implementation detail. Do not use directly.
    
    This class is not part of the public API and may change without notice.
    
    Validator for Snapshot layer ETL processes.
    
    Handles validation specific to Snapshot layer operations including:
    - Point-in-time capture validation
    - Historical data integrity checks
    - Temporal consistency validation
    - Snapshot retention policy validation
    """
    pass