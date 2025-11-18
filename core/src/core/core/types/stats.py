"""Core types for statistics management.

These types are used across multiple layers for statistics configuration
and management. They provide a common data structure for representing
statistics metadata that can be used by compute, medallion, datalake,
and other modules.
"""

from typing import Dict, List
from pydantic import BaseModel, Field


class StatsConfiguration(BaseModel):
    """Complete statistics configuration for a schema.
    
    This model contains all statistics configuration for tables within a
    specific database schema, providing methods to query and analyze the
    configuration. It can be used by any layer (compute, medallion, datalake).
    
    This is a core type that represents statistics metadata independently
    of where or how the statistics are created or managed.
    
    Attributes:
        schema_name: Name of the database schema (e.g., 'bronze', 'silver', 'gold')
        table_stats: Mapping of table names to lists of column names that need statistics
        
    Example:
        >>> config = StatsConfiguration(
        >>>     schema_name="bronze",
        >>>     table_stats={
        >>>         "InventTrans": ["ItemId", "DatePhysical", "Qty"],
        >>>         "SalesTable": ["SalesId", "CustAccount"]
        >>>     }
        >>> )
        >>> 
        >>> # Check if table has stats
        >>> if config.has_table("InventTrans"):
        >>>     columns = config.get_table_columns("InventTrans")
        >>>     print(f"Create stats on columns: {columns}")
    """
    
    schema_name: str = Field(
        ...,
        description="Database schema name (e.g., 'bronze', 'silver', 'gold')"
    )
    
    table_stats: Dict[str, List[str]] = Field(
        default_factory=dict,
        description="Mapping of table names to column lists for statistics"
    )
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """Get statistics columns for a specific table.
        
        Args:
            table_name: Name of the table to query
            
        Returns:
            List of column names needing statistics, empty list if not found
            
        Example:
            >>> columns = config.get_table_columns("InventTrans")
            >>> # Returns ["ItemId", "DatePhysical", "Qty"]
        """
        return self.table_stats.get(table_name, [])
    
    def has_table(self, table_name: str) -> bool:
        """Check if a table has statistics configuration.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if the table has statistics configured with at least one column
            
        Example:
            >>> if config.has_table("InventTrans"):
            >>>     # Table has statistics configuration
            >>>     pass
        """
        return table_name in self.table_stats and len(self.table_stats[table_name]) > 0
    
    def get_tables(self) -> List[str]:
        """Get list of all tables with statistics configuration.
        
        Returns:
            List of table names that have statistics configuration
            
        Example:
            >>> tables = config.get_tables()
            >>> # Returns ["InventTrans", "SalesTable"]
        """
        return list(self.table_stats.keys())
    
    def get_total_columns(self) -> int:
        """Get the total number of columns across all tables.
        
        Returns:
            Total count of columns that need statistics
            
        Example:
            >>> total = config.get_total_columns()
            >>> print(f"Total columns needing stats: {total}")
        """
        return sum(len(columns) for columns in self.table_stats.values())
    
    def merge(self, other: 'StatsConfiguration') -> None:
        """Merge another configuration into this one.
        
        Tables from the other configuration are added or updated in this one.
        If a table exists in both, the other configuration's columns replace
        the existing ones.
        
        Args:
            other: Another StatsConfiguration to merge
            
        Example:
            >>> config1.merge(config2)
            >>> # config1 now contains tables from both configurations
        """
        if other.schema_name != self.schema_name:
            raise ValueError(
                f"Cannot merge configurations from different schemas: "
                f"{self.schema_name} != {other.schema_name}"
            )
        
        self.table_stats.update(other.table_stats)
    
    def __str__(self) -> str:
        """String representation of the configuration."""
        table_count = len(self.table_stats)
        column_count = self.get_total_columns()
        return (
            f"StatsConfiguration(schema={self.schema_name}, "
            f"tables={table_count}, columns={column_count})"
        )
    
    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"StatsConfiguration(schema_name={self.schema_name!r}, table_stats={self.table_stats!r})"