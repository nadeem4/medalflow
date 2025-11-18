"""Azure Synapse Analytics compute engines.

This module provides engine implementations specific to Azure Synapse Analytics,
including both SQL and Spark engines for executing queries and jobs.

Synapse-Specific Features:
    - Serverless SQL pool support
    - Dedicated SQL pool support
    - Apache Spark pool integration
    - External table management
    - PolyBase data loading

Engines:
    - SynapseSQLEngine: Executes T-SQL queries on SQL pools
    - SynapseSparkEngine: Submits and monitors Spark jobs

Connection Methods:
    - SQL: ODBC/pyodbc for SQL pool connections
    - Spark: REST API for job submission
    - Authentication: Azure AD, SQL Auth, MSI

Performance Optimizations:
    - Connection pooling
    - Query timeout management
    - Automatic retry logic
    - Batch execution support

Example:
    from core.compute.engines.synapse import SynapseSQLEngine
    from core.settings import SynapseSettings
    
    settings = SynapseSettings(
        server="mysynapse.sql.azuresynapse.net",
        database="mydatabase",
        authentication="AzureAD"
    )
    
    engine = SynapseSQLEngine(settings)
    
    # Test connection
    if engine.test_connection():
        # Execute query
        engine.execute_query(
            "CREATE EXTERNAL TABLE bronze.customers "
            "WITH (LOCATION = 'customers/') "
            "AS SELECT * FROM staging.customers"
        )
"""

from core.compute.engines.synapse.sql_engine import SynapseSQLEngine
from core.compute.engines.synapse.spark_engine import SynapseSparkEngine

__all__ = ["SynapseSQLEngine", "SynapseSparkEngine"]