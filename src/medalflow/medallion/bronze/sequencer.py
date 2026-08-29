"""Bronze layer sequencers for raw data ingestion.

The Bronze layer ingests raw data from source systems with minimal
transformation. Two sequencers do it, and they differ in exactly one thing --
where the list of source tables comes from:

* :class:`BronzeSequencer` reads it off its own ``@bronze_metadata``. One model
  is one table, nothing is queried, and a plan therefore compiles offline
  (ADR 002, Decision D6). This is the default.
* :class:`IntrospectedBronzeSequencer` queries ``INFORMATION_SCHEMA`` for every
  table in a source schema. This was bronze's only mode; it is now the
  documented opt-in alternative, selected by ``bronze_introspection``.

Everything downstream of that list -- the CTAS, the soft-delete filter, the
statistics -- is generated identically for both.
"""

import logging
from functools import cached_property
from typing import TYPE_CHECKING

from medalflow.constants.medallion import Layer
from medalflow.constants.sql import QueryType
from medalflow.operations import BaseOperation, CreateTable, Select
from medalflow.query_builder.factory import create_query_builder
from medalflow.types import QueryMetadata
from medalflow.types.metadata import BronzeMetadata

from ..base.sequencer import _BaseSequencer
from ..landing_zone.lake_database import LakeDatabase
from ..types import TableInfo

if TYPE_CHECKING:
    from medalflow.settings import MedalflowSettings

logger = logging.getLogger(__name__)


class BronzeSequencer(_BaseSequencer):
    """Builds one Bronze table from one declared Bronze model.

    Decorate a subclass with ``@bronze_metadata`` and it is a complete model:
    the decorator names the target table and schema, and the source table and
    schema the rows are read from. Discovery finds it by walking the configured
    bronze package, exactly as silver and gold are found.

    Attributes:
        selection: Optional list of target table names to restrict this
            sequencer to. None means the model's table; an empty list means none.
        source_schema: Default source schema, used by any model that does not
            declare a ``source_schema`` of its own
        lake_db: LakeDatabase instance, built on first use. A declared model
            never touches it; :class:`IntrospectedBronzeSequencer` does.
        table_prefix: Prefix to add to bronze table names (inherited from base)
    """

    def __init__(
        self,
        settings: "MedalflowSettings",
        selection: list[str] | None = None,
        *,
        source_schema: str = "dbo",
    ):
        """Initialize the Bronze sequencer.

        Args:
            settings: Configuration settings for the sequencer
            selection: Optional list of target table names to process. None
                means every table this sequencer declares.
            source_schema: Default source schema (default: "dbo")
        """
        super().__init__(settings, selection)

        self.source_schema = source_schema
        self.layer = Layer.BRONZE

    @cached_property
    def lake_db(self) -> LakeDatabase:
        """The source lake database, opened on first use.

        Built lazily so that constructing a sequencer stays offline (D6): a
        plan can be described, and its constructor exercised, without a
        warehouse to connect to.
        """
        return LakeDatabase(self.settings, self.source_schema)

    def get_layer_name(self) -> str:
        """Return the layer name for this sequencer.

        Returns:
            'bronze' - the bronze layer identifier
        """
        return self.layer.value

    def get_obj_name(self) -> str:
        """The model's declared name, or the class name when undecorated.

        Never raises: `create_plan_from_sequencers` calls this while reporting
        a failure, so a missing decorator must not mask the real error.

        Returns:
            The declared name, falling back to the class name
        """
        metadata = getattr(type(self), "_bronze_metadata", None)

        return metadata.name if metadata else super().get_obj_name()

    def _get_class_metadata_attribute(self) -> str | None:
        """Get the class-level metadata attribute name for Bronze sequencers.

        Returns:
            '_bronze_metadata' - the attribute the decorator attaches
        """
        return "_bronze_metadata"

    # --- what this model declares -------------------------------------------

    def _declared_metadata(self) -> BronzeMetadata:
        """The metadata `@bronze_metadata` attached to this class.

        Returns:
            The declared BronzeMetadata

        Raises:
            ValueError: If the class carries no declaration. Bronze models are
                declared by default; introspecting a warehouse instead is
                `IntrospectedBronzeSequencer`, chosen explicitly.
        """
        metadata: BronzeMetadata | None = getattr(type(self), "_bronze_metadata", None)

        if metadata is None:
            raise ValueError(
                f"{type(self).__name__} declares no bronze model. Decorate it with "
                f"@bronze_metadata(name=..., schema=..., source_system=...), or use "
                f"IntrospectedBronzeSequencer to derive tables from a live warehouse."
            )

        return metadata

    @property
    def target_schema(self) -> str:
        """Schema the bronze table is created in."""
        return self._declared_metadata().schema

    def _source_tables(self) -> list[TableInfo]:
        """The source tables this sequencer ingests -- the seam between modes.

        A declared model is one table, read from its own ``source_schema`` when
        it names one and the sequencer's otherwise.

        Returns:
            The model's source table, or nothing when `selection` excludes it
        """
        metadata = self._declared_metadata()

        if self.selection is not None and metadata.name not in self.selection:
            logger.info(f"Bronze model {metadata.name} is not in the requested selection; skipping")
            return []

        schema = metadata.source_schema or self.source_schema

        return [
            TableInfo(
                table_name=metadata.source_table,
                schema_name=schema,
                full_table_name=f"{schema}.{metadata.source_table}",
            )
        ]

    def _target_table_name(self, source: TableInfo) -> str:
        """Bronze table one source table is ingested into.

        Args:
            source: TableInfo for the source table

        Returns:
            The model's declared name, which may differ from the source's
        """
        return self._declared_metadata().name

    # --- turning a source table into operations -----------------------------

    def _create_table_op(self, table: TableInfo) -> CreateTable:
        """Create execution plan for a single table using operations.

        Args:
            table: TableInfo object for the source table

        Returns:
            CreateTable
        """
        # Create SELECT operation for source data
        select_op = self._create_select_operation(table)

        query_builder = create_query_builder()
        select_sql = query_builder.build_query(select_op)

        target_name = self._target_table_name(table)
        target_schema = self.target_schema

        # Create CREATE TABLE operation (CTAS)
        create_op = CreateTable(
            operation_type=QueryType.CREATE_TABLE,
            schema_name=target_schema,
            object_name=target_name,
            select_query=select_sql,
            recreate=True,
            logging_context={"table": table.full_table_name, "layer": self.layer.value},
            metadata=QueryMetadata(
                table_name=target_name,
                create_stats=True,
                # Was `self.layer`, a `Layer` member in a `str` field: it only
                # validated because `Layer` subclasses `str`. It is the write
                # target, which is the declared schema, not the layer name.
                schema_name=target_schema,
                type=QueryType.CREATE_TABLE,
            ),
        )

        return create_op

    def _create_select_operation(self, table: TableInfo) -> Select:
        """Create SELECT operation for source data.

        Args:
            table: TableInfo object for the source table

        `columns` is left unset, which `Select` documents as SELECT *. It used
        to be `["*"]`, and the builder validates every named column against an
        identifier whitelist -- so bronze raised `Invalid identifier name: *`
        before it could render a single query. Nothing caught it because
        rendering was only reachable behind a live warehouse.

        Returns:
            Select operation for the source data
        """
        return Select(
            operation_type=QueryType.SELECT,
            schema_name=table.schema_name,
            object_name=table.table_name,
            where_clause=self._soft_delete_filter(table.table_name),
        )

    def _soft_delete_filter(self, table_name: str) -> str | None:
        """WHERE clause hiding soft-deleted rows, if that convention is configured.

        Bronze used to filter every source table on a hardcoded ``IsDelete IS
        NULL`` and exempt every table whose name ended in ``Metadata`` -- two
        assumptions about one warehouse's schema, applied to all of them. Both
        now come from ``conventions.soft_delete``, and unset means bronze reads
        its sources whole.

        Args:
            table_name: Unqualified source table name

        Returns:
            The configured predicate, or None when the convention is unset or
            the table is exempt from it
        """
        convention = self.settings.conventions.soft_delete
        if convention is None or not convention.applies_to(table_name):
            return None

        return convention.predicate

    def get_queries(self) -> list[BaseOperation]:
        """Build one CTAS per source table.

        Returns:
            List of CreateTable operations, one per source table
        """
        tables = self._source_tables()

        logger.info(f"Processing {len(tables)} source tables for the bronze layer")

        return [self._create_table_op(table) for table in tables]


class IntrospectedBronzeSequencer(BronzeSequencer):
    """Builds a Bronze table for every table in a live source schema.

    This was bronze's only mode: `INFORMATION_SCHEMA` decided which tables
    existed, which meant compiling a plan required a warehouse. It is kept, as
    an opt-in alternative for teams that want it, and is reached by setting
    ``MEDALFLOW_BRONZE_INTROSPECTION=true``.

    Attributes:
        target_schema: Schema every introspected table is created in
    """

    def __init__(
        self,
        settings: "MedalflowSettings",
        selection: list[str] | None = None,
        *,
        source_schema: str = "dbo",
        target_schema: str = "bronze",
    ):
        """Initialize the introspecting Bronze sequencer.

        Args:
            settings: Configuration settings for the sequencer
            selection: Optional list of table names to process. None means
                every table in the source schema; an empty list means none.
            source_schema: Source schema to introspect (default: "dbo")
            target_schema: Schema the bronze tables are created in. There is no
                declaration to read one off, so it is configured here.
        """
        super().__init__(settings, selection, source_schema=source_schema)

        self._target_schema = target_schema

    @property
    def target_schema(self) -> str:
        """Schema every introspected table is created in."""
        return self._target_schema

    def _source_tables(self) -> list[TableInfo]:
        """Ask the warehouse which tables the source schema holds.

        Returns:
            Every table in the source schema, narrowed to `selection`
        """
        if self.selection is not None and not self.selection:
            # `LakeDatabase.get_tables([])` treats an empty list as "no filter"
            # and returns everything. `[]` means no tables, in every layer.
            logger.info("Empty selection: introspecting no tables for the bronze layer")
            return []

        tables = self.lake_db.get_tables(table_names=self.selection)

        if self.selection:
            logger.info(f"Processing {len(tables)} requested tables for bronze layer")
        else:
            logger.info(
                f"Processing all {len(tables)} tables from {self.source_schema} for bronze layer"
            )

        return tables

    def _target_table_name(self, source: TableInfo) -> str:
        """An introspected table keeps its source name.

        Args:
            source: TableInfo for the source table

        Returns:
            The source table's own name
        """
        return source.table_name
