"""Bronze layer sequencer for raw data ingestion.

This module provides the BronzeSequencer class for Bronze layer ETL processes.
The Bronze layer is responsible for ingesting raw data from source systems
with minimal transformation.
"""

import logging
from functools import cached_property
from typing import TYPE_CHECKING

from medalflow.constants.medallion import Layer
from medalflow.constants.sql import QueryType
from medalflow.operations import BaseOperation, CreateTable, Select
from medalflow.query_builder.factory import create_query_builder
from medalflow.types import QueryMetadata

from ..base.sequencer import _BaseSequencer
from ..landing_zone.lake_database import LakeDatabase
from ..types import TableInfo

if TYPE_CHECKING:
    from medalflow.settings import MedalflowSettings

logger = logging.getLogger(__name__)


class BronzeSequencer(_BaseSequencer):
    """Sequencer for Bronze layer ETL processes.

    The BronzeSequencer handles raw data ingestion from source systems,
    focusing on data extraction and initial validation while preserving
    the original data format. It generates execution plans including
    table creation queries and statistics generation.

    Attributes:
        selection: Optional list of specific tables to process
        source_schema: Source database schema name
        lake_db: LakeDatabase instance for accessing source tables, built on
            first use
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
            selection: Optional list of table names to process. None means
                every table in the source schema.
            source_schema: Source schema name (default: "dbo")
        """
        super().__init__(settings)

        self.selection = selection
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

        # Create CREATE TABLE operation (CTAS)
        create_op = CreateTable(
            operation_type=QueryType.CREATE_TABLE,
            schema_name="bronze",
            object_name=table.table_name,
            select_query=select_sql,
            recreate=True,
            logging_context={"table": table.full_table_name, "layer": self.layer.value},
            metadata=QueryMetadata(
                table_name=table.table_name,
                create_stats=True,
                schema_name=self.layer,
                type=QueryType.CREATE_TABLE,
            ),
        )

        return create_op

    def _create_select_operation(self, table: TableInfo) -> Select:
        """Create SELECT operation for source data.

        Args:
            table: TableInfo object for the source table

        Returns:
            Select operation for the source data
        """
        return Select(
            operation_type=QueryType.SELECT,
            schema_name=self.source_schema,
            object_name=table.table_name,
            columns=["*"],
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
        tables = self.lake_db.get_tables(table_names=self.selection)

        if self.selection:
            logger.info(f"Processing {len(tables)} requested tables for bronze layer")
        else:
            logger.info(
                f"Processing all {len(tables)} tables from {self.source_schema} for bronze layer"
            )

        table_plans: list[CreateTable] = []
        for table in tables:
            plan = self._create_table_op(table)
            table_plans.append(plan)

        return table_plans
