from core.observability.context import sanitize_extras
from core.logging import get_logger
from typing import List, Optional, Dict, TYPE_CHECKING
from core.compute.factory import get_platform_factory
from core.constants.compute import ComputeEnvironment, ResultFormat
from core.core.features.registry import get_feature_manager
from ..types import TableInfo
from core.protocols import CacheProtocol
from core.operations import ExecuteSQL


if TYPE_CHECKING:
    from core.settings import _Settings

logger = get_logger(__name__)

class LakeDatabase:

    def __init__(self, settings: "_Settings", schema: str = "dbo"):
        self.settings = settings
        self.compute_settings = self.settings.compute
        self.name = self.compute_settings.active_config.lake_database_name
        self.schema = schema
        self._platform = None
        self._cache_manager: CacheProtocol = get_feature_manager('cache')



    def get_lake_database_name(self) -> str:
        """Get the name of the lake database."""
        return self.name


    def get_table_full_name(self, table_name: str) -> str:
        """Get the full table name including database and schema."""
        return f"{self.name}.{self.schema}.{table_name}"
    
    def _get_cache_key(self) -> str:
        """Generate cache key for this database/schema combination."""
        return f"lakedatabase:{self.name}:{self.schema}:tables"
    
    def _get_platform(self):
        """Lazy load platform."""
        if not self._platform:
            self._platform = get_platform_factory().create(environment=ComputeEnvironment.ETL)
        return self._platform
    
    def _get_query_to_fetch_tables(self) -> str:
        """Construct SQL query to fetch all table names in the lake database schema."""
        return f"""
           SELECT table_name, table_schema as schema_name
        FROM {self.name}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{self.schema}' AND TABLE_CATALOG = '{self.name}'
        AND table_name not like ('%_partitioned') 
        """
    
    def get_tables(self, table_names: Optional[List[str]] = None, refresh: bool = False) -> List[TableInfo]:
        """
        Retrieve tables from the specified schema with caching via CacheManager.
        
        Args:
            table_names: Optional list of specific table names to retrieve. 
                        If None, returns all tables. If specified, validates and returns only those tables.
            refresh: Force refresh the cache if True
            
        Returns:
            List[TableInfo]: All tables or filtered list of requested tables
            
        Raises:
            ValueError: If any requested table doesn't exist
        """
        cache_key = self._get_cache_key()
        all_tables: List[TableInfo] = []
        
        if refresh and self._cache_manager:
            self._cache_manager.delete(cache_key)
            logger.info(
                "lake_database.cache_cleared",
                extra=sanitize_extras({
                    "cache_key": cache_key,
                    "database": self.name,
                    "schema": self.schema,
                }),
            )
        
        if self._cache_manager:
            all_tables = self._cache_manager.get(
                key=cache_key,
                loader=self._fetch_tables_from_db
            )
            if all_tables:
                logger.debug(
                    "lake_database.tables_cache_hit",
                    extra=sanitize_extras({
                        "database": self.name,
                        "schema": self.schema,
                        "table_count": len(all_tables),
                    }),
                )
        else:
            logger.debug(
                "lake_database.tables_cache_miss",
                extra=sanitize_extras({"database": self.name, "schema": self.schema}),
            )
            all_tables = self._fetch_tables_from_db()
        
        if not table_names:
            return all_tables

        all_tables_dict: Dict[str, TableInfo] = {t.table_name.lower(): t for t in all_tables}
        result_tables: List[TableInfo] = []
        missing_tables = []
        
        for name in table_names:
            name_lower = name.lower()
            if name_lower in all_tables_dict:
                result_tables.append(all_tables_dict[name_lower])
            else:
                missing_tables.append(name)
        
        if missing_tables:
            raise ValueError(
                f"Tables not found in {self.name}.{self.schema}: {', '.join(missing_tables)}"
            )
        
        logger.debug(
            "lake_database.tables_filtered",
            extra=sanitize_extras({
                "database": self.name,
                "schema": self.schema,
                "result_count": len(result_tables),
            }),
        )
        return result_tables
    
    def _fetch_tables_from_db(self) -> List[TableInfo]:
        """Fetch tables from database (internal method for cache loader)."""
        query = self._get_query_to_fetch_tables()
        platform = self._get_platform()
        
        logger.info(
            "lake_database.fetch_tables", 
            extra=sanitize_extras({"database": self.name, "schema": self.schema}),
        )
        result = platform.execute_sql_query(query, result_format=ResultFormat.DICT_LIST)
        
        if not result.success:
            raise RuntimeError(f"Failed to fetch tables: {result.error_message}")
        
        results = result.data or []
        
        tables = [
            TableInfo(
                table_name=row['table_name'],
                schema_name=row['schema_name'], 
                full_table_name=f'{self.name}.{row["schema_name"]}.{row["table_name"]}'
            )
            for row in results
        ]
        
        logger.info(
            "lake_database.tables_fetched",
            extra=sanitize_extras({
                "database": self.name,
                "schema": self.schema,
                "table_count": len(tables),
            }),
        )
        
        if self._cache_manager:
            self._cache_manager.set(self._get_cache_key(), tables)
            
        return tables
    
    def validate_tables(self, table_names: List[str]) -> Dict[str, bool]:
        """
        Check which tables exist in the database using cached data.
        
        Args:
            table_names: List of table names to validate
            
        Returns:
            Dict mapping table name to existence (True/False)
        """
        all_tables = self.get_tables()  # Uses cache
        existing_tables = {t.table_name.lower() for t in all_tables}
        return {
            name: name.lower() in existing_tables 
            for name in table_names
        }
    
    def refresh_cache(self):
        """Force refresh the table cache."""
        if self._cache_manager:
            cache_key = self._get_cache_key()
            self._cache_manager.delete(cache_key)
            logger.info(
                "lake_database.cache_cleared",
                extra=sanitize_extras({
                    "cache_key": cache_key,
                    "database": self.name,
                    "schema": self.schema,
                }),
            )
        # Pre-fetch to populate cache
        self.get_tables(refresh=True)
    





