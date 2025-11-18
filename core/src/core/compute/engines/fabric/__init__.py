"""Microsoft Fabric compute engines.

This module provides engine implementations for Microsoft Fabric, supporting
both SQL (Data Warehouse) and Spark (Data Engineering) experiences.

Fabric-Specific Features:
    - Lakehouse integration
    - Data Warehouse SQL engine
    - Spark compute with Delta Lake
    - OneLake storage integration
    - Cross-workspace queries

Engines:
    - FabricSQLEngine: Executes SQL queries in Fabric Data Warehouse
    - FabricSparkEngine: Manages Spark jobs in Fabric notebooks/jobs

Key Capabilities:
    - Delta Lake table management
    - Managed table support
    - Direct Lake mode for Power BI
    - V-Order optimization
    - Automatic file compaction

Authentication:
    - Azure AD authentication
    - Workspace identity
    - Service principal support

Example:
    from core.compute.engines.fabric import FabricSQLEngine
    from core.settings import FabricSettings
    
    settings = FabricSettings(
        workspace_id="my-workspace-id",
        lakehouse_id="my-lakehouse-id",
        sql_endpoint="my-warehouse.datawarehouse.fabric.microsoft.com"
    )
    
    engine = FabricSQLEngine(settings)
    
    # Create a managed Delta table
    engine.execute_query(
        "CREATE TABLE IF NOT EXISTS silver.fact_sales "
        "USING DELTA "
        "AS SELECT * FROM bronze.sales WHERE year >= 2023"
    )
"""

from .sql_engine import FabricSQLEngine
from .spark_engine import FabricSparkEngine

__all__ = ["FabricSQLEngine", "FabricSparkEngine"]