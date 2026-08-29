"""Gold layer sequencer -- the published, query-facing layer.

:class:`GoldSequencer` is the thinnest of the three. It supplies its layer
name, the ``_gold_metadata`` attribute discovery looks for, and the name
lookup that reads it. Everything that turns ``@query_metadata`` methods into
an ordered plan is inherited unchanged from
:class:`~medalflow.medallion.base.sequencer._BaseSequencer`.

Nothing here is gold-specific beyond that, which is the point: a gold model
publishing a view says so with ``QueryType.CREATE_OR_ALTER_VIEW`` in its own
``@query_metadata``, and what it publishes stays the author's decision rather
than this class's.
"""

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

    def get_obj_name(self) -> str:
        """The model's declared name, or the class name when undecorated.

        Never raises: it is the cache key and the log name, and it is read
        while reporting failures, so a missing decorator must not mask the
        real error.

        Returns:
            The declared name, falling back to the class name
        """
        metadata = getattr(type(self), "_gold_metadata", None)

        return metadata.name if metadata else super().get_obj_name()

    def _get_class_metadata_attribute(self) -> str | None:
        """Get the class-level metadata attribute name for Gold sequencer.

        Gold sequencer uses @gold_metadata decorator at class level.

        Returns:
            '_gold_metadata' - the attribute name for class metadata
        """
        return "_gold_metadata"
