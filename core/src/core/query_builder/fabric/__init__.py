"""Fabric query builder implementations.

This module contains query builders for Microsoft Fabric platforms,
including Data Warehouse and Spark implementations.

The query builders generate platform-specific SQL for Fabric,
handling Delta tables, OneLake integration, and Fabric-specific features.

Example:
    # Preferred: Use factory for auto-configuration
    from core.query_builder import get_fabric_query_builder
    from core.operations import CreateTable
    
    # Factory handles all configuration automatically
    builder = get_fabric_query_builder()
    
    operation = CreateTable(
        schema="silver",
        object_name="customers",
        select_query="SELECT * FROM bronze.raw_customers"
    )
    
    sql = builder.build_query(operation)
    
    # Alternative: Manual configuration (for testing/custom setups)
    from core.query_builder.fabric import FabricWarehouseQueryBuilder
    
    builder = FabricWarehouseQueryBuilder(table_prefix="oracle_")
"""

from core.query_builder.fabric.warehouse_builder import FabricWarehouseQueryBuilder

__all__ = [
    "FabricWarehouseQueryBuilder"
]