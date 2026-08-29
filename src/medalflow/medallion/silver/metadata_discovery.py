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

    Silver is the one layer whose discovery filters on the configured model
    list: `model=` is its grouping concept, and a transformation whose model
    the deployment has not configured is left out of the plan.

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

    def get_transformations_by_models(self, models: str) -> list[TransformationMetadata]:
        """Get all transformations for a specific model.

        Uses cache for improved performance when available.

        Args:
            models: Comma-separated model names, or 'all'

        Returns:
            List of transformations for the specified model
        """
        if models.lower() == "all":
            return self.discover_all_transformations()

        wanted = [model.strip().lower() for model in models.strip().split(",")]
        all_transformations = self.discover_all_transformations()

        result = [metadata for metadata in all_transformations if metadata.model.lower() in wanted]

        self.logger.debug(f"Found {len(result)} transformations for models: {wanted}")

        return result

    def get_transformations_by_names(self, names: str) -> list[TransformationMetadata]:
        """Get transformations by name.

        Uses cache for improved performance when available.

        Args:
            names: Comma-separated transformation names

        Returns:
            Every transformation whose name matches, empty if none do
        """
        wanted = [name.strip().lower() for name in names.strip().split(",")]

        all_transformations = self.discover_all_transformations()

        result = [metadata for metadata in all_transformations if metadata.name.lower() in wanted]

        self.logger.debug(f"Found {len(result)} transformations for names: {wanted}")

        return result

    def _extract_metadata_from_class(self, cls: type) -> TransformationMetadata | None:
        """Extract and normalize metadata from a decorated class.

        Args:
            cls: Decorated class

        Returns:
            TransformationMetadata, or None if the transformation is disabled
            or its model is not configured for this deployment

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
