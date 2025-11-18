import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import pandas as pd

from core.constants.compute import ComputeEnvironment, EngineType, ResultFormat
from core.constants.sql import QueryType
from core.compute.engines.base import BaseSQLEngine, BaseSparkEngine
from core.query_builder.base import BaseQueryBuilder
from core.compute.types import OperationResult, BatchOperationResult
from core.common.exceptions import query_execution_error
from core.operations import (
    BaseOperation,
    QueryContext,
    CreateTable,
    Insert,
    Update,
    Delete,
    Merge,
    CreateStatistics,
    CreateOrAlterView,
    ExecuteSQL,
    OperationBuilder
)
from core.logging import get_logger

if TYPE_CHECKING:
    from core.settings import BaseComputeSettings


logger = get_logger(__name__)


class _BasePlatform(ABC):
    """Internal implementation detail. Do not use directly.
    
    This class is not part of the public API and may change without notice.
    
    Base class for compute platforms.
    
    A platform represents a specific compute service (Synapse, Fabric)
    and handles all database operations in a platform-agnostic way.
    """
    
    def __init__(
        self,
        settings: 'BaseComputeSettings',
        environment: ComputeEnvironment = ComputeEnvironment.ETL
    ):
        """Initialize platform with settings.
        
        Args:
            settings: Platform-specific compute settings
            environment: Compute environment (ETL or CONSUMPTION)
        """
        self.settings = settings
        self.environment = environment
        
        # Initialize engines to None - will be set by _initialize_dependencies
        self._sql_engine: Optional[BaseSQLEngine] = None
        self._spark_engine: Optional[BaseSparkEngine] = None
        self._query_builder: Optional[BaseQueryBuilder] = None
        
        # Initialize platform-specific dependencies
        self._initialize_dependencies()
    
    @abstractmethod
    def name(self) -> str:
        """Get platform name."""
        pass
    
    @abstractmethod
    def supported_engines(self) -> List[EngineType]:
        """Get list of supported engine types."""
        pass
    
    @abstractmethod
    def _initialize_dependencies(self) -> None:
        """Initialize platform-specific dependencies.
        
        This method is called during platform initialization to set up
        all required dependencies such as SQL engines, Spark engines,
        query builders, and data lake clients.
        
        Concrete platforms must implement this method to create their
        specific dependencies.
        """
        pass
    
    
    def execute_operation(
        self,
        operation: BaseOperation,
        telemetry: Optional[Dict[str, str]] = None,
    ) -> OperationResult:
        """Execute a database operation."""
        start_time = time.time()
        operation_payload = operation.telemetry_fields()
        telemetry_payload: Dict[str, str] = dict(telemetry or {})
        telemetry_payload.update(operation_payload)

        try:
            engine_type = self._select_engine_for_operation(operation)
            query = self._query_builder.build_query(operation)

            if engine_type == EngineType.SPARK:
                result = self._execute_with_spark(query, operation, telemetry_payload)
            else:
                result = self._execute_with_sql(query, operation, telemetry_payload)

            result.duration_seconds = time.time() - start_time
            result.engine_used = engine_type

            if (
                result.success
                and operation.operation_type == QueryType.CREATE_TABLE
                and isinstance(operation, CreateTable)
                and operation.metadata
                and operation.metadata.create_stats
            ):
                stats_op = CreateStatistics(
                    schema_name=operation.schema_name,
                    object_name=operation.object_name,
                    stats_name=f"stats_{operation.object_name}_auto",
                    with_fullscan=True,
                    auto_discover=True,
                )
                stats_telemetry = {**telemetry_payload, **stats_op.telemetry_fields()}
                try:
                    stats_query = self._query_builder.build_query(stats_op)
                    stats_result = self._execute_with_sql(stats_query, stats_op, stats_telemetry)
                    if not stats_result.success:
                        logger.warning(
                            "Failed to create statistics",
                            extra={**stats_telemetry, "error_message": stats_result.error_message or "unknown"},
                        )
                    else:
                        logger.info(
                            "Successfully created statistics",
                            extra=stats_telemetry,
                        )
                except Exception as stats_error:
                    logger.warning(
                        "Error creating statistics",
                        extra={**stats_telemetry, "error": str(stats_error)},
                    )

            return result

        except Exception as exc:
            duration = time.time() - start_time
            engine_value = None
            if "engine_type" in locals() and isinstance(engine_type, EngineType):
                engine_value = engine_type.value
            logger.error(
                "Platform operation failed",
                extra={**telemetry_payload, "engine.used": engine_value or "unknown"},
                exc_info=True,
            )
            return OperationResult(
                success=False,
                operation_type=operation.operation_type,
                schema_name=operation.schema_name,
                object_name=operation.object_name,
                duration_seconds=duration,
                error_message=str(exc),
                error_type=type(exc).__name__,
            )
    
    
    def execute(self, operation_dict: dict, telemetry: Optional[Dict[str, str]] = None) -> OperationResult:
        operation = OperationBuilder.create_operation_from_dict(operation_dict)

        return self.execute_operation(operation, telemetry=telemetry)
    
    def execute_sql_query(
        self,
        sql: str,
        return_results: bool = True,
        result_format: ResultFormat = ResultFormat.DATAFRAME,
    ) -> OperationResult:
        
        operation = ExecuteSQL(
            sql=sql,
            returns_results=return_results,
            result_format=result_format if return_results else ResultFormat.DATAFRAME,
            schema_name="",  
            object_name="" 
        )
        
        return self.execute_operation(operation)
    
    
    def _get_sql_engine(self) -> BaseSQLEngine:
        """Get SQL engine instance (internal use only).
        
        Note: Concrete platform implementations should ensure sql_engine 
        is always provided by the builder as a required dependency.
        """
        if self._sql_engine is None:
            raise RuntimeError(
                "SQL engine not configured. Use platform builder to create platform with dependencies."
            )
        return self._sql_engine
    
    def _get_spark_engine(self) -> BaseSparkEngine:
        """Get Spark engine instance (internal use only)."""
        if self._spark_engine is None:
            raise RuntimeError(
                "Spark engine not configured. Use platform builder to create platform with dependencies."
            )
        return self._spark_engine
    
    def _get_query_builder(self) -> BaseQueryBuilder:
        """Get query builder instance (internal use only).
        
        Note: Concrete platform implementations should ensure query_builder
        is always provided by the builder as a required dependency.
        """
        if self._query_builder is None:
            raise RuntimeError(
                "Query builder not configured. Use platform builder to create platform with dependencies."
            )
        return self._query_builder
    
    
    def _select_engine_for_operation(self, operation: BaseOperation) -> EngineType:
        """Select the appropriate engine for an operation.
        
        Args:
            operation: The operation to execute
            
        Returns:
            Selected engine type
        """
        if operation.engine_hint and operation.engine_hint != EngineType.AUTO:
            if operation.engine_hint in self.supported_engines():
                return operation.engine_hint
            else:
                logger.warning(
                    f"Requested engine {operation.engine_hint} not available, using AUTO"
                )
        
        if operation.operation_type in [
            QueryType.CREATE_STATISTICS,
            QueryType.CREATE_SCHEMA,
            QueryType.CREATE_OR_ALTER_VIEW,
        ]:
            return EngineType.SQL
        
        if operation.operation_type in [QueryType.MERGE, QueryType.COPY]:
            if EngineType.SPARK in self.supported_engines():
                return EngineType.SPARK
        
        if isinstance(operation, (Insert, Update, Delete)):
            pass
        
        if EngineType.SQL in self.supported_engines():
            return EngineType.SQL
        
        engines = [e for e in self.supported_engines() if e != EngineType.AUTO]
        if engines:
            return engines[0]
        
        raise ValueError(f"No engines available for platform {self.name()}")
    

    
    def _execute_with_sql(
        self,
        query: str,
        operation: BaseOperation,
        telemetry: Optional[Dict[str, str]] = None,
    ) -> OperationResult:
        """Execute query with SQL engine."""
        engine = self._get_sql_engine()
        telemetry_payload = dict(telemetry or {})

        try:
            if (
                operation.operation_type == QueryType.EXECUTE_SQL
                and hasattr(operation, "returns_results")
                and operation.returns_results
            ):
                result_format = getattr(operation, "result_format", ResultFormat.DATAFRAME)

                if result_format == ResultFormat.DICT_LIST:
                    data = engine.fetch_all(query, telemetry=telemetry_payload)
                    rows = len(data)
                elif result_format == ResultFormat.SCALAR:
                    data = engine.fetch_scalar(query, telemetry=telemetry_payload)
                    rows = 1 if data is not None else 0
                else:
                    data = engine.fetch_dataframe(query, telemetry=telemetry_payload)
                    rows = len(data)

                return OperationResult(
                    success=True,
                    operation_type=operation.operation_type,
                    schema_name=operation.schema_name,
                    object_name=operation.object_name,
                    duration_seconds=0,
                    rows_affected=rows,
                    data=data,
                    query_executed=query,
                )

            engine.execute_query(query, telemetry=telemetry_payload)
            return OperationResult(
                success=True,
                operation_type=operation.operation_type,
                schema_name=operation.schema_name,
                object_name=operation.object_name,
                duration_seconds=0,
                rows_affected=None,
                query_executed=query,
            )

        except Exception as exc:
            logger.error(
                "SQL execution failed",
                extra={**telemetry_payload, "error": str(exc)},
                exc_info=True,
            )
            return OperationResult(
                success=False,
                operation_type=operation.operation_type,
                schema_name=operation.schema_name,
                object_name=operation.object_name,
                duration_seconds=0,
                error_message=str(exc),
                error_type=type(exc).__name__,
                query_executed=query,
            )
    
    def _execute_with_spark(
        self,
        query: str,
        operation: BaseOperation,
        telemetry: Optional[Dict[str, str]] = None,
    ) -> OperationResult:
        """Execute query with Spark engine.
        
        Args:
            query: SQL query to execute
            operation: Original operation for context
            
        Returns:
            BaseOperation result
        """
        # Return not implemented error for now
        return OperationResult(
            success=False,
            operation_type=operation.operation_type,
            schema_name=operation.schema_name,
            object_name=operation.object_name,
            duration_seconds=0,
            error_message="Executing queries with Spark is not yet supported",
            error_type="NotImplementedError",
            query_executed=query
        )
    
    def _supports_transactions(self) -> bool:
        """Check if platform supports transactions.
        
        Default is False. Override in platforms that support it.
        """
        return False
    
    def _begin_transaction(self) -> None:
        """Begin a transaction.
        
        Override in platforms that support transactions.
        """
        pass
    
    def _commit_transaction(self) -> None:
        """Commit a transaction.
        
        Override in platforms that support transactions.
        """
        pass
    
    def _rollback_transaction(self) -> None:
        """Rollback a transaction.
        
        Override in platforms that support transactions.
        """
        pass
    
    def test_connection(self) -> Dict[str, bool]:
        """Test connections for all available engines.
        
        Returns:
            Dictionary of engine type to connection status
        """
        results = {}
        
        if EngineType.SQL in self.supported_engines():
            try:
                sql_engine = self._get_sql_engine()
                results["sql"] = sql_engine.test_connection()
            except Exception as e:
                logger.error(f"SQL connection test failed: {e}")
                results["sql"] = False
        
        if EngineType.SPARK in self.supported_engines():
            try:
                spark_engine = self._get_spark_engine()
                results["spark"] = spark_engine.test_connection()
            except Exception as e:
                logger.error(f"Spark connection test failed: {e}")
                results["spark"] = False
        
        return results
    
    
    def cleanup(self) -> None:
        """Clean up resources."""
        self._sql_engine = None
        self._spark_engine = None
        self._query_builder = None
