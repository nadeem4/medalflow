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
    - base.py: Abstract base class for all builders

Design Principles:
    1. **SQL Generation Only**: Builders only generate SQL strings
    2. **Platform-Specific**: Each platform has its own SQL dialect
    3. **Security First**: Input validation prevents SQL injection
    4. **Stateless**: Builders don't maintain state between calls
    5. **Operation-Based**: All builders work with Operation types

Available Builders:
    - SynapseServerlessQueryBuilder: For Synapse Serverless SQL pools

Example:
    >>> from medalflow.query_builder import create_query_builder
    >>> from medalflow.operations import CreateTable
    >>> 
    >>> # Use factory to get configured builder
    >>> builder = create_query_builder()
    >>> 
    >>> # Generate CREATE TABLE SQL
    >>> operation = CreateTable(
    ...     schema_name="silver",
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

Platform Specifics:
    Synapse:
        - External tables with LOCATION, DATA_SOURCE, FILE_FORMAT
        - CREATE STATISTICS with FULLSCAN
        - T-SQL specific syntax

See Also:
    - medalflow.query_builder.base: Base query builder interface
    - medalflow.compute.engines: Query execution engines
    - medalflow.compute.platforms: Platform implementations
"""

# Re-export key classes for convenience
from medalflow.query_builder.base import BaseQueryBuilder
from medalflow.query_builder.factory import (
    create_query_builder,
)
from medalflow.query_builder.synapse.serverless_builder import SynapseServerlessQueryBuilder

__all__ = [
    "BaseQueryBuilder",
    "create_query_builder",
    "SynapseServerlessQueryBuilder",
]