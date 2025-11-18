from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from core.medallion.base.sequencer import _BaseSequencer
from core.constants.medallion import Layer, SnapshotFrequency
from core.settings import get_settings


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
    
    def _requires_class_metadata(self) -> bool:
        """Snapshot sequencer requires class-level metadata.
        
        Returns:
            True - Snapshot sequencer must have @snapshot_metadata decorator
        """
        return True
    
    
