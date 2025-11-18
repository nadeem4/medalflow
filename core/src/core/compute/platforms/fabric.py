"""Microsoft Fabric platform implementation.

This module provides the platform implementation for Microsoft Fabric,
enabling seamless integration with Fabric's lakehouse and warehouse capabilities.

Key Features:
    - Native Delta Lake support
    - Managed tables by default (vs external in Synapse)
    - Integrated lakehouse/warehouse experience
    - Direct Lake mode optimization
"""

from typing import Optional

from core.constants.compute import EngineType
from core.compute.engines.fabric import FabricSQLEngine, FabricSparkEngine
from core.compute.platforms.base import _BasePlatform
from core.query_builder.fabric import FabricWarehouseQueryBuilder
from core.datalake.client import DatalakeClient
from core.datalake import get_processed_datalake_client
from core.settings import FabricSettings, ComputeEnvironment
from core.query_builder import get_fabric_query_builder


class FabricPlatform(_BasePlatform):
    """Internal implementation detail. Do not use directly.
    
    This class is not part of the public API and may change without notice.
    
    Microsoft Fabric platform implementation.
    
    Fabric uses managed tables in lakehouses rather than external tables.
    It provides both SQL and Spark engines for data processing.
    """
    
    def __init__(self, 
                 settings: FabricSettings,
                 environment: ComputeEnvironment = ComputeEnvironment.ETL):
        """Initialize Fabric platform.
        
        Args:
            settings: Fabric settings from configuration
            environment: Compute environment (ETL or CONSUMPTION)
        """
        if not isinstance(settings, FabricSettings):
            raise TypeError("Settings must be FabricSettings")
     
        super().__init__(settings=settings, environment=environment)
    
    def name(self) -> str:
        """Get platform name."""
        return "fabric"
    
    def supported_engines(self) -> list[EngineType]:
        """Get list of supported engine types.
        
        Fabric supports both SQL (via SQL Analytics endpoint) and Spark engines.
        """
        return [EngineType.SQL, EngineType.SPARK, EngineType.AUTO]
    
    def _initialize_dependencies(self) -> None:
        """Initialize Fabric-specific dependencies.
        
        Creates SQL engine, query builder, and data lake client.
        """
 
        self._sql_engine = FabricSQLEngine(self.settings, self.environment)
        self._query_builder = get_fabric_query_builder()
        self._spark_engine: Optional[FabricSparkEngine] = None
        
