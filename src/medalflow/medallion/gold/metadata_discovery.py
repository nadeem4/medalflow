"""Gold layer model discovery.

Gold's entry point used to build a *bare* `GoldSequencer`, which carries no
`@query_metadata` methods of its own -- so a user's `@gold_metadata` class was
never found and the plan came back empty. Gold now walks its configured
package the same way silver walks its own.
"""

from dataclasses import dataclass

from medalflow.medallion.base.discovery import _BaseDiscovery
from medalflow.types import GoldMetadata

from .sequencer import GoldSequencer


@dataclass
class GoldModelMetadata:
    """One discovered gold model.

    Attributes:
        name: The model's identity. `gold_metadata` carries no `name` yet
            (ADR 002 D2 lands it in a later PR), so the class name stands in --
            it is what the plan reports and what discovery keys on.
        sequencer_class: The decorated class, to be constructed with
            `(settings, selection)`.
        gold_metadata: The metadata the decorator attached.
    """

    name: str
    sequencer_class: type[GoldSequencer]
    gold_metadata: GoldMetadata

    @property
    def description(self) -> str:
        """Get description from gold metadata."""
        return self.gold_metadata.description or ""

    @property
    def tags(self) -> list[str]:
        """Get tags from gold metadata."""
        return self.gold_metadata.tags or []

    @property
    def module_path(self) -> str:
        """Get full module path of the sequencer class."""
        return f"{self.sequencer_class.__module__}.{self.sequencer_class.__name__}"


class GoldMetadataDiscovery(_BaseDiscovery):
    """Discovers the gold models declared in the gold package.

    Attributes:
        package: Package name for gold models
        settings: Application settings
        logger: Logger instance
        _cache_manager: Global cache manager for caching metadata
    """

    layer = "gold"
    metadata_attribute = "_gold_metadata"

    def _extract_metadata_from_class(self, cls: type) -> GoldModelMetadata | None:
        """Extract metadata from a decorated gold class.

        `settings.is_model_configured` is deliberately NOT applied here. It is
        backed by `configured_models`, silver's grouping concept: gold models
        declare no `model=`, so gating on it would silently drop every gold
        model unless the deployment listed it under a setting documented as
        silver's.

        Args:
            cls: Decorated class

        Returns:
            GoldModelMetadata, or None if the model is disabled

        Raises:
            Exception: Surfaced to the caller. A model whose metadata cannot be
                read is an authoring error, not a reason to drop it.
        """
        try:
            meta: GoldMetadata = cls._gold_metadata

            if meta.disabled:
                self.logger.debug(f"Skipping disabled gold model: {cls.__name__}")
                return None

            return GoldModelMetadata(
                name=cls.__name__,
                sequencer_class=cls,
                gold_metadata=meta,
            )

        except Exception as e:
            self.logger.error(f"Failed to extract metadata from {cls.__name__}: {e}")
            raise
