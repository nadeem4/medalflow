"""Bronze layer model discovery.

Bronze used to have no discovery at all: `get_bronze_execution_plan` built one
sequencer and asked a live `INFORMATION_SCHEMA` query which tables existed. A
declared bronze model was never found, because nothing looked for one.

Bronze now walks its configured package the same way silver and gold do; the
walk itself lives in `medalflow.medallion.base.discovery`.

Introspection did not go away, it moved here. `IntrospectedBronzeDiscovery`
answers the same question the walk answers -- which bronze models exist -- from
a live `INFORMATION_SCHEMA` query, and returns the same records, so the mode is
invisible to everything downstream of discovery.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from medalflow.logging import get_logger
from medalflow.medallion.base.discovery import _BaseDiscovery
from medalflow.types import BronzeMetadata

from ..landing_zone.lake_database import LakeDatabase
from ..types import TableInfo
from .sequencer import BronzeSequencer

if TYPE_CHECKING:
    from medalflow.settings import MedalflowSettings


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


class IntrospectedBronzeDiscovery:
    """Derives one bronze model per table in a live source schema.

    The opt-in alternative of ADR 002, Decision 6, reached by setting
    ``MEDALFLOW_BRONZE_INTROSPECTION=true``. It answers the same question the
    package walk answers -- *which bronze models exist* -- from
    ``INFORMATION_SCHEMA`` instead of from ``@bronze_metadata`` declarations.

    **Turning it on costs offline compile for the bronze layer.** That is the
    trade-off the decision named when it made introspection opt-in: deriving
    tables from a live query means compiling needs a warehouse. Silver and
    gold are untouched, and the default mode stays offline.

    Introspection belongs here rather than in a sequencer because it is a
    discovery question, and putting it here is what makes the two modes
    interchangeable everywhere downstream: each table becomes an ordinary
    :class:`BronzeModelMetadata` whose declaration was derived rather than
    written, so a selector matches it exactly as it matches a declared model
    and `compile()` and `run()` stay the single path.

    The schema is queried once, whatever the selection: narrowing happens on
    the models, which is where a selector can see it.

    Attributes:
        settings: Application settings
        source_schema: Schema introspected for tables
        target_schema: Schema every introspected table is created in. There is
            no declaration to read one off, so it is configured here.
        logger: Logger instance
    """

    layer = "bronze"

    def __init__(
        self,
        package: str | None = None,
        settings: "MedalflowSettings | None" = None,
        *,
        source_schema: str = "dbo",
        target_schema: str = "bronze",
    ):
        """Initialize the introspecting discovery.

        Args:
            package: Accepted and ignored, so this stands in for the package
                walk without its caller having to know which mode is active.
                An introspecting project configures no bronze package.
            settings: Application settings. Resolved from the global singleton
                when not supplied.
            source_schema: Schema to introspect
            target_schema: Schema the bronze tables are created in
        """
        if settings is None:
            from medalflow.settings import get_settings

            settings = get_settings()

        self.settings = settings
        self.source_schema = source_schema
        self.target_schema = target_schema
        self.logger = get_logger(self.__class__.__name__)

    def discover_all(self, force_refresh: bool = False) -> list[BronzeModelMetadata]:
        """Ask the warehouse which tables the source schema holds.

        Args:
            force_refresh: Accepted for signature parity with the package
                walk. Introspection queries every time regardless: there is no
                walk to cache, and a stale table list is the failure mode this
                mode exists to avoid.

        Returns:
            One model per table in the source schema

        Raises:
            Exception: Whatever reaching the warehouse raised. `compile()`
                turns it into a structured error; nothing is swallowed here.
        """
        tables = LakeDatabase(self.settings, self.source_schema).get_tables()

        self.logger.info(f"Introspected {len(tables)} tables from {self.source_schema}")

        return [self._model(table) for table in tables]

    def _model(self, table: TableInfo) -> BronzeModelMetadata:
        """Turn one introspected table into a bronze model.

        The synthesized class is a plain :class:`BronzeSequencer` carrying a
        derived ``_bronze_metadata``, so everything downstream -- the CTAS, the
        soft-delete filter, the statistics -- is generated by the declared
        path. An introspected table keeps its own name.

        Args:
            table: The introspected source table

        Returns:
            The model, indistinguishable from a declared one
        """
        metadata = BronzeMetadata(
            name=table.table_name,
            schema=self.target_schema,
            source_system=self.settings.source_system,
            source_table=table.table_name,
            source_schema=table.schema_name,
            description=f"Introspected from {table.full_table_name}",
        )

        return BronzeModelMetadata(
            name=metadata.name,
            sequencer_class=type(
                table.table_name, (BronzeSequencer,), {"_bronze_metadata": metadata}
            ),
            bronze_metadata=metadata,
        )
