"""Bronze layer sequencer for raw data ingestion.

This module provides the BronzeSequencer class for Bronze layer ETL processes.
The Bronze layer is responsible for ingesting raw data from source systems
with minimal transformation.
"""

from typing import Dict, List, Optional, TYPE_CHECKING
import logging

from core.operations import BaseOperation, CreateTable, CreateStatistics, Select
from core.query_builder.factory import QueryBuilderFactory
from core.constants.sql import QueryType
from ..base.sequencer import _BaseSequencer
from ..landing_zone.lake_database import LakeDatabase
from ..types import TableInfo, LineageInfo
from core.constants.medallion import Layer
from core.types import QueryMetadata


if TYPE_CHECKING:
    from core.settings import _Settings

logger = logging.getLogger(__name__)


class BronzeSequencer(_BaseSequencer):
    """Sequencer for Bronze layer ETL processes.
    
    The BronzeSequencer handles raw data ingestion from source systems,
    focusing on data extraction and initial validation while preserving
    the original data format. It generates execution plans including
    table creation queries and statistics generation.
    
    Attributes:
        source_schema: Source database schema name
        lake_db: LakeDatabase instance for accessing source tables
        table_prefix: Prefix to add to bronze table names (inherited from base)
        requested_table_names: Optional list of specific tables to process
    """
    
    def __init__(
        self,
        settings: "_Settings", 
        schema: str = "dbo",
        table_names: Optional[str] = None
    ):
        """Initialize the Bronze sequencer.
        
        Args:
            settings: Configuration settings for the sequencer
            schema: Source schema name (default: "dbo")
            table_names: Optional comma-separated list of table names to process
        """
        super().__init__(settings)
        
        self.source_schema = schema
        self.lake_db = LakeDatabase(settings, schema)
        self.requested_table_names = self._parse_table_names(table_names)
        self.layer = Layer.BRONZE
    
    def _parse_table_names(self, table_names: Optional[str]) -> Optional[List[str]]:
        """Parse comma-separated table names into a list.
        
        Args:
            table_names: Comma-separated string of table names
            
        Returns:
            List of table names or None if not provided
        """
        if not table_names:
            return None
        # Handle comma-separated values, trim whitespace
        names = [name.strip() for name in table_names.split(',') if name.strip()]
        return names if names else None

    
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
        
        query_builder = QueryBuilderFactory.create()
        select_sql = query_builder.build_query(select_op)
        
        # Create CREATE TABLE operation (CTAS)
        create_op = CreateTable(
            operation_type=QueryType.CREATE_TABLE,
            schema="bronze",
            object_name=table.table_name,  
            select_query=select_sql,
            recreate=True,
            logging_context={"table": table.full_table_name, "layer": self.layer.value},
            metadata=QueryMetadata(table_name=table.table_name, create_stats=True, schema_name=self.layer, type=QueryType.CREATE_TABLE)

        )
        
        return create_op
    
    def _create_select_operation(self, table: TableInfo) -> Select:
        """Create SELECT operation for source data.
        
        Args:
            table: TableInfo object for the source table
            
        Returns:
            Select operation for the source data
        """
        where_clause = None if table.table_name.endswith("Metadata") else "IsDelete IS NULL"
        
        return Select(
            operation_type=QueryType.SELECT,
            schema=self.source_schema,
            object_name=table.table_name,
            columns=["*"],
            where_clause=where_clause
        )
    
    

    def get_queries(self) -> List[BaseOperation]:
        tables = self.lake_db.get_tables(table_names=self.requested_table_names)
        
        if self.requested_table_names:
            logger.info(f"Processing {len(tables)} requested tables for bronze layer")
        else:
            logger.info(f"Processing all {len(tables)} tables from {self.source_schema} for bronze layer")
        
        table_plans: List[CreateTable] = []
        for table in tables:
            plan = self._create_table_op(table)
            table_plans.append(plan)

    
        return table_plans