"""Query builder module for SQL generation across compute platforms.

This module provides query builders for generating platform-specific SQL
statements. Query builders are responsible for translating operations into
SQL but do NOT execute queries - that's handled by engines.

This module is in Layer 1 (Infrastructure) as it provides fundamental
SQL generation capabilities that can be used by multiple Layer 2 modules,
not just compute platforms.

Architecture:
    The query builder module is organized by platform:
    - synapse/: Azure Synapse query builders
    - fabric/: Microsoft Fabric query builders
    - base.py: Abstract base class for all builders

Design Principles:
    1. **SQL Generation Only**: Builders only generate SQL strings
    2. **Platform-Specific**: Each platform has its own SQL dialect
    3. **Security First**: Input validation prevents SQL injection
    4. **Stateless**: Builders don't maintain state between calls
    5. **Operation-Based**: All builders work with Operation types

Available Builders:
    - SynapseServerlessQueryBuilder: For Synapse Serverless SQL pools
    - SynapseSQLQueryBuilder: For Synapse Dedicated SQL pools
    - FabricWarehouseQueryBuilder: For Fabric Warehouse
    - FabricSparkQueryBuilder: For Fabric Spark

Example:
    >>> from core.query_builder import get_synapse_query_builder
    >>> from core.operations import CreateTable
    >>> 
    >>> # Use factory to get configured builder
    >>> builder = get_synapse_query_builder()
    >>> 
    >>> # Generate CREATE TABLE SQL
    >>> operation = CreateTable(
    ...     schema="silver",
    ...     object_name="customers",
    ...     select_query="SELECT * FROM bronze.raw_customers"
    ... )
    >>> sql = builder.build_query(operation)
    >>> print(sql)
    CREATE EXTERNAL TABLE [silver].[customers]
    WITH (
        DATA_SOURCE = ProcessedDataSource,
        LOCATION = 'silver/customers/',
        FILE_FORMAT = ParquetFileFormat
    )
    AS SELECT * FROM bronze.raw_customers

Security:
    All query builders inherit from BaseQueryBuilder which provides:
    - Identifier validation (schema, table, column names)
    - SQL injection protection through regex validation
    - Safe string escaping and quoting
    - Maximum length constraints on identifiers

Platform Differences:
    Synapse:
        - External tables with LOCATION, DATA_SOURCE, FILE_FORMAT
        - CREATE STATISTICS with FULLSCAN
        - T-SQL specific syntax
        
    Fabric:
        - Managed tables with USING format
        - ANALYZE TABLE for statistics
        - Spark SQL and Warehouse SQL variants

See Also:
    - core.query_builder.base: Base query builder interface
    - core.compute.engines: Query execution engines
    - core.compute.platforms: Platform implementations
"""

# Re-export key classes for convenience
from core.query_builder.base import BaseQueryBuilder
from core.query_builder.factory import (
    QueryBuilderFactory,
    get_query_builder,
    get_synapse_query_builder,
    get_fabric_query_builder,
)
from core.query_builder.synapse.serverless_builder import SynapseServerlessQueryBuilder
from core.query_builder.fabric.warehouse_builder import FabricWarehouseQueryBuilder

__all__ = [
    "BaseQueryBuilder",
    "QueryBuilderFactory",
    "get_query_builder",
    "get_synapse_query_builder",
    "get_fabric_query_builder",
    "SynapseServerlessQueryBuilder",
    "FabricWarehouseQueryBuilder",
]