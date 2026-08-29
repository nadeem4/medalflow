"""Silver layer model discovery.

Silver transformations are discovered by walking a configured Python package
and reading the metadata `@silver_metadata` attached to each class -- no CSV,
no JSON, no external configuration file. The walk itself is shared with the
other layers; see `medalflow.medallion.base.discovery`.
"""

from dataclasses import dataclass

from medalflow.medallion.base.discovery import _BaseDiscovery
from medalflow.types import SilverMetadata

from .sequencer import SilverTransformationSequencer


@dataclass
class TransformationMetadata:
    """Metadata for a discovered silver transformation.

    Only contains essential fields. Other fields are accessed via properties
    from the stored silver_metadata object.
    """

    name: str
    model: str
    sequencer_class: type[SilverTransformationSequencer]
    silver_metadata: SilverMetadata

    @property
    def description(self) -> str:
        """Get description from silver metadata."""
        return self.silver_metadata.description or ""

    @property
    def tags(self) -> list[str]:
        """Get tags from silver metadata."""
        return self.silver_metadata.tags or []

    @property
    def module_path(self) -> str:
        """Get full module path of the sequencer class."""
        return f"{self.sequencer_class.__module__}.{self.sequencer_class.__name__}"


class SilverMetadataDiscovery(_BaseDiscovery):
    """Discovers the silver transformations declared in the silver package.

    Silver is the one layer whose discovery can filter on the configured model
    list: `model=` is its grouping concept. The filter narrows only -- an unset
    `configured_models` keeps every transformation, because a filter nobody
    configured must not delete the layer it filters.

    Attributes:
        package: Package name for silver transformations
        settings: Application settings
        logger: Logger instance
        _cache_manager: Global cache manager for caching metadata
    """

    layer = "silver"
    metadata_attribute = "_silver_metadata"

    def discover_all_transformations(
        self, force_refresh: bool = False
    ) -> list[TransformationMetadata]:
        """Discover all silver transformations from the package.

        Args:
            force_refresh: Force re-discovery even if cache exists

        Returns:
            List of discovered transformation metadata
        """
        return self.discover_all(force_refresh=force_refresh)

    def _extract_metadata_from_class(self, cls: type) -> TransformationMetadata | None:
        """Extract and normalize metadata from a decorated class.

        Args:
            cls: Decorated class

        Returns:
            TransformationMetadata, or None if the transformation is disabled
            or a configured model list is in force and does not name its model

        Raises:
            Exception: Surfaced to the caller. A model whose metadata cannot be
                read is an authoring error, not a reason to drop it.
        """
        try:
            meta: SilverMetadata = cls._silver_metadata

            if meta.disabled:
                self.logger.debug(f"Skipping disabled transformation: {meta.name}")
                return None

            if not self.settings.is_model_configured(meta.model):
                self.logger.debug(
                    f"Skipping transformation {meta.name}: model '{meta.model}' not configured"
                )
                return None

            return TransformationMetadata(
                name=meta.name,
                model=meta.model,
                sequencer_class=cls,
                silver_metadata=meta,
            )

        except Exception as e:
            self.logger.error(f"Failed to extract metadata from {cls.__name__}: {e}")
            raise
