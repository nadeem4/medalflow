"""Snapshot layer components for point-in-time data capture.

The Snapshot layer captures and preserves data at specific points in time
for compliance, auditing, and historical analysis. This layer supports
various snapshot frequencies and retention policies.

Components:
    - SnapshotSequencer: Sequencer for snapshot data capture
    - SnapshotProcessor: Processor for snapshot operations
    - SnapshotValidator: Validator for snapshot data integrity
    - snapshot_metadata: Decorator for snapshot configuration
"""

from .sequencer import SnapshotSequencer
from .decorators import snapshot_metadata
from .processor import _SnapshotProcessor as SnapshotProcessor
from .validator import _SnapshotValidator as SnapshotValidator

__all__ = [
    "SnapshotSequencer",
    "SnapshotProcessor",
    "SnapshotValidator",
    "snapshot_metadata",
]