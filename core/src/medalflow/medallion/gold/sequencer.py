from typing import List, Optional

from medalflow.medallion.base.sequencer import _BaseSequencer
from medalflow.constants.medallion import Layer
from medalflow.settings import get_settings
from medalflow.types.metadata import DiscoveredMethod
from medalflow.operations import BaseOperation


class GoldSequencer(_BaseSequencer):
    """Sequencer for Gold layer operations in the medallion architecture."""
    
    def __init__(self, selected_tables: Optional[List[str]] = None):
        """Initialize the Gold sequencer.
        
        Args:
            selected_tables: Optional list of table names to process.
                            If None, all tables are processed.
        """
        settings = get_settings()
        super().__init__(settings)
        self.layer = Layer.GOLD
        self.selected_tables = selected_tables
    
    def get_layer_name(self) -> str:
        """Return the layer name for this sequencer.
        
        Returns:
            'gold' - the gold layer identifier
        """
        return self.layer.value
    
    def _get_class_metadata_attribute(self) -> Optional[str]:
        """Get the class-level metadata attribute name for Gold sequencer.
        
        Gold sequencer uses @gold_metadata decorator at class level.
        
        Returns:
            '_gold_metadata' - the attribute name for class metadata
        """
        return '_gold_metadata'
    
    def _get_queries(self, discovered_methods: List[DiscoveredMethod]) -> List[BaseOperation]:
        """Filter operations based on selected table names.
        
        Args:
            discovered_methods: List of discovered methods with metadata and SQL
            
        Returns:
            List[BaseOperation]: Filtered list of operations
        """
        # If no selection (None), process all tables
        # If empty list, process nothing
        if self.selected_tables is None:
            return super()._get_queries(discovered_methods)
        
        # Filter discovered methods by table name
        filtered_methods = [
            method for method in discovered_methods
            if method.metadata.table_name in self.selected_tables
        ]
        
        if not filtered_methods:
            self.logger.warning(
                f"No methods found for selected tables: {self.selected_tables}"
            )
        
        return super()._get_queries(filtered_methods)
    
    
