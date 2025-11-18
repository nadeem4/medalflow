"""Azure Synapse Analytics platform implementation.

This module provides the platform implementation for Azure Synapse Analytics,
managing SQL and Spark engines along with Synapse-specific query builders.

Key Features:
    - Automatic engine selection based on workload
    - External table support for data lake integration
    - Serverless and dedicated SQL pool support
    - Spark pool integration when configured
"""

import time
from typing import Dict, Optional

import pandas as pd

from core.constants.compute import EngineType
from core.constants.sql import QueryType
from core.compute.engines.synapse import SynapseSQLEngine, SynapseSparkEngine
from core.compute.platforms.base import _BasePlatform
from core.query_builder.synapse import SynapseServerlessQueryBuilder
from core.compute.types import OperationResult
from core.operations import (
    BaseOperation, Delete, Update, Insert, DropTable, CreateTable
)
from core.datalake import get_processed_datalake_client
from core.datalake.client import DatalakeClient
from core.settings import SynapseSettings, ComputeEnvironment, get_settings
from core.query_builder import get_synapse_query_builder
from core.logging import get_logger

logger = get_logger(__name__)


class SynapsePlatform(_BasePlatform):
    """Internal implementation detail. Do not use directly.
    
    This class is not part of the public API and may change without notice.
    
    Azure Synapse Analytics platform implementation."""
    
    def __init__(self, 
                 settings: SynapseSettings,
                 environment: ComputeEnvironment = ComputeEnvironment.ETL):
        """Initialize Synapse platform.
        
        Args:
            settings: Synapse settings from configuration
            environment: Compute environment (ETL or CONSUMPTION)
        """
        if not isinstance(settings, SynapseSettings):
            raise TypeError("Settings must be SynapseSettings")
        

        # Call base constructor which will call _initialize_dependencies
        super().__init__(settings=settings, environment=environment)
    
    def supported_engines(self) -> list[EngineType]:
        """Get list of supported engine types."""
        engines = []
        
        # SQL engine always available (uses ODBC connections)
        engines.append(EngineType.SQL)
        
        # Spark engine available if configured
        if self.settings.spark_configured:
            engines.append(EngineType.SPARK)
        
        # AUTO is always supported if any engine is available
        engines.append(EngineType.AUTO)
        
        return engines

    def name(self) -> str:
        """Get platform name."""
        return "synapse"
    
    def _initialize_dependencies(self) -> None:
        """Initialize Synapse-specific dependencies.
        
        Creates SQL engine, query builder, and data lake client.
        """
       
        self._sql_engine = SynapseSQLEngine(self.settings, self.environment)
            
        self._query_builder = get_synapse_query_builder()
        self._datalake_client = get_processed_datalake_client()
        self._spark_engine: Optional[SynapseSparkEngine] = None
    
    def execute_operation(
        self,
        operation: BaseOperation,
        telemetry: Optional[Dict[str, str]] = None,
    ) -> OperationResult:
        """Execute a database operation with Synapse-specific handling.
        
        Handles deletion of underlying data lake files when recreating external tables.
        Fails the operation if data deletion fails to ensure consistency.
        
        Args:
            operation: The operation to execute
            
        Returns:
            Result of the operation execution
        """
        start_time = time.time()
        telemetry_payload = dict(telemetry or {})
        
        # Handle Synapse-specific data deletion for external tables with recreate=True
        if (operation.operation_type == QueryType.CREATE_TABLE and
            isinstance(operation, CreateTable) and
            operation.recreate and 
            operation.location):
            
            # Delete underlying data lake directory before recreating table
            try:
                logger.info(
                    "Deleting underlying data",
                    extra={**telemetry_payload, "datalake.path": str(operation.location)},
                )
                self._datalake_client.delete(operation.location)
                logger.info(
                    "Successfully deleted data",
                    extra={**telemetry_payload, "datalake.path": str(operation.location)},
                )
            except Exception as e:
                # Fail the operation if we can't delete the data
                logger.error(
                    "Failed to delete underlying data",
                    extra={**telemetry_payload, "datalake.path": str(operation.location), "error": str(e)},
                    exc_info=True,
                )
                return OperationResult(
                    success=False,
                    operation_type=operation.operation_type,
                    schema_name=operation.schema_name,
                    object_name=operation.object_name,
                    duration_seconds=time.time() - start_time,
                    error_message=f"Cannot recreate table: Failed to delete existing data at {operation.location}: {str(e)}",
                    error_type="DataDeletionError"
                )
        
        # Call parent class implementation for actual operation execution
        return super().execute_operation(operation, telemetry=telemetry_payload)
