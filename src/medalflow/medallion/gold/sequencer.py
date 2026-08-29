from typing import TYPE_CHECKING

from medalflow.constants.medallion import Layer
from medalflow.medallion.base.sequencer import _BaseSequencer

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
        super().__init__(settings, selection)
        self.layer = Layer.GOLD

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
