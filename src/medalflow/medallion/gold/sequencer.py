from typing import TYPE_CHECKING

from medalflow.constants.medallion import Layer
from medalflow.medallion.base.sequencer import _BaseSequencer
from medalflow.operations import BaseOperation
from medalflow.types.metadata import DiscoveredMethod

if TYPE_CHECKING:
    from medalflow.settings import MedalflowSettings


class GoldSequencer(_BaseSequencer):
    """Sequencer for Gold layer operations in the medallion architecture."""

    def __init__(self, settings: "MedalflowSettings", selection: list[str] | None = None):
        """Initialize the Gold sequencer.

        Args:
            settings: Configuration settings for the sequencer
            selection: Optional list of table names to process. None means
                every table; an empty list means none.
        """
        super().__init__(settings)
        self.layer = Layer.GOLD
        self.selection = selection

    def get_layer_name(self) -> str:
        """Return the layer name for this sequencer.

        Returns:
            'gold' - the gold layer identifier
        """
        return self.layer.value

    def _get_class_metadata_attribute(self) -> str | None:
        """Get the class-level metadata attribute name for Gold sequencer.

        Gold sequencer uses @gold_metadata decorator at class level.

        Returns:
            '_gold_metadata' - the attribute name for class metadata
        """
        return "_gold_metadata"

    def _get_queries(self, discovered_methods: list[DiscoveredMethod]) -> list[BaseOperation]:
        """Filter operations based on selected table names.

        Args:
            discovered_methods: List of discovered methods with metadata and SQL

        Returns:
            List[BaseOperation]: Filtered list of operations
        """
        # If no selection (None), process all tables
        # If empty list, process nothing
        if self.selection is None:
            return super()._get_queries(discovered_methods)

        # Filter discovered methods by table name
        filtered_methods = [
            method for method in discovered_methods if method.metadata.table_name in self.selection
        ]

        if not filtered_methods:
            self.logger.warning(f"No methods found for selected tables: {self.selection}")

        return super()._get_queries(filtered_methods)
