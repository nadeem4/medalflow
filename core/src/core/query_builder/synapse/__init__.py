"""Synapse query builder implementations.

This module contains query builders for Azure Synapse Analytics platforms,
including Serverless SQL pools and Dedicated SQL pools.

The query builders generate platform-specific T-SQL for Synapse,
handling external tables, OPENROWSET, and PolyBase operations.

Example:
    # Preferred: Use factory for auto-configuration
    from core.query_builder import get_synapse_query_builder
    from core.operations import CreateTable
    
    # Factory handles all configuration automatically
    builder = get_synapse_query_builder()
    
    operation = CreateTable(
        schema="silver",
        object_name="customers",
        select_query="SELECT * FROM bronze.raw_customers"
    )
    
    sql = builder.build_query(operation)
    
    # Alternative: Manual configuration (for testing/custom setups)
    from core.query_builder.synapse import SynapseServerlessQueryBuilder
    from core.settings import _Settings
    
    settings = _Settings()  # Loads from environment or config
    
    builder = SynapseServerlessQueryBuilder(settings)
"""

from core.query_builder.synapse.serverless_builder import (
    SynapseServerlessQueryBuilder
)

__all__ = [
    "SynapseServerlessQueryBuilder"
]