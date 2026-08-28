from typing import Optional

from medalflow.medallion.base.sequencer import _BaseSequencer
from medalflow.constants.medallion import Layer
from medalflow.settings import get_settings


class SnapshotSequencer(_BaseSequencer):
    
    def __init__(self):
        """Initialize the Snapshot sequencer."""
        settings = get_settings()
        super().__init__(settings)
        self.layer = Layer.SNAPSHOT
    
    def get_layer_name(self) -> str:
        """Return the layer name for this sequencer.
        
        Returns:
            'snapshot' - the snapshot layer identifier
        """
        return self.layer.value
    
    def _get_class_metadata_attribute(self) -> Optional[str]:
        """Get the class-level metadata attribute name for Snapshot sequencer.
        
        Snapshot sequencer uses @snapshot_metadata decorator at class level.
        
        Returns:
            '_snapshot_metadata' - the attribute name for class metadata
        """
        return '_snapshot_metadata'
