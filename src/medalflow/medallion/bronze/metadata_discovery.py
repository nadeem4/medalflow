"""Bronze layer model discovery.

Bronze used to have no discovery at all: `get_bronze_execution_plan` built one
sequencer and asked a live `INFORMATION_SCHEMA` query which tables existed. A
declared bronze model was never found, because nothing looked for one.

Bronze now walks its configured package the same way silver and gold do; the
walk itself lives in `medalflow.medallion.base.discovery`.
"""

from dataclasses import dataclass

from medalflow.medallion.base.discovery import _BaseDiscovery
from medalflow.types import BronzeMetadata

from .sequencer import BronzeSequencer


@dataclass
class BronzeModelMetadata:
    """One discovered bronze model.

    Attributes:
        name: The model's identity, taken from `@bronze_metadata(name=...)`.
            It is what the plan reports, what discovery keys on, and the name
            of the bronze table the model builds.
        sequencer_class: The decorated class, to be constructed with
            `(settings, selection)`.
        bronze_metadata: The metadata the decorator attached.
    """

    name: str
    sequencer_class: type[BronzeSequencer]
    bronze_metadata: BronzeMetadata

    @property
    def description(self) -> str:
        """Get description from bronze metadata."""
        return self.bronze_metadata.description or ""

    @property
    def tags(self) -> list[str]:
        """Get tags from bronze metadata."""
        return self.bronze_metadata.tags or []

    @property
    def module_path(self) -> str:
        """Get full module path of the sequencer class."""
        return f"{self.sequencer_class.__module__}.{self.sequencer_class.__name__}"


class BronzeMetadataDiscovery(_BaseDiscovery):
    """Discovers the bronze models declared in the bronze package.

    Attributes:
        package: Package name for bronze models
        settings: Application settings
        logger: Logger instance
        _cache_manager: Global cache manager for caching metadata
    """

    layer = "bronze"
    metadata_attribute = "_bronze_metadata"

    def _extract_metadata_from_class(self, cls: type) -> BronzeModelMetadata | None:
        """Extract metadata from a decorated bronze class.

        `settings.is_model_configured` is deliberately NOT applied here, for the
        same reason it is not applied to gold. It is backed by
        `configured_models`, silver's grouping concept: bronze models declare no
        `model=`, so gating on it would silently drop every bronze model unless
        the deployment listed it under a setting documented as silver's.

        Args:
            cls: Decorated class

        Returns:
            BronzeModelMetadata, or None if the model is disabled

        Raises:
            Exception: Surfaced to the caller. A model whose metadata cannot be
                read is an authoring error, not a reason to drop it.
        """
        try:
            meta: BronzeMetadata = cls._bronze_metadata

            if meta.disabled:
                self.logger.debug(f"Skipping disabled bronze model: {meta.name}")
                return None

            return BronzeModelMetadata(
                name=meta.name,
                sequencer_class=cls,
                bronze_metadata=meta,
            )

        except Exception as e:
            self.logger.error(f"Failed to extract metadata from {cls.__name__}: {e}")
            raise
