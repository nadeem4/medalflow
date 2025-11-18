import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from core.settings import BaseComputeSettings
from urllib import parse

import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.pool import QueuePool

from core.common.exceptions import CTEError, connection_error, query_execution_error
from core.constants.compute import ComputeEnvironment
from core.compute.types import JobResult, JobStatus, SparkJobConfig
from core.utils.decorators import retry_with_backoff as retry, traced
from core.logging import get_logger

logger = get_logger(__name__)


class BaseSQLEngine:
    """SQLAlchemy-based SQL execution engine for all platforms.
    
    This concrete implementation provides full SQL engine functionality using
    SQLAlchemy, which supports Synapse, Fabric, Databricks, Snowflake, and many
    other platforms. Platform-specific engines inherit from this class and only
    need to provide customization through hooks.
    
    Features:
        - Automatic connection pooling with SQLAlchemy
        - Optimized fetch methods (scalar, all, dataframe)
        - Built-in retry logic for reliability
        - Comprehensive error handling and logging
        - ODBC-based connections for maximum compatibility
        - Platform customization through hooks
    
    Platform Customization:
        Subclasses can override these hooks:
        - _apply_connection_settings(): Apply platform-specific SET commands
        - get_connection_info(): Return platform-specific connection details
    
    Supported Platforms:
        Any SQLAlchemy-compatible database including:
        - Azure Synapse
        - Microsoft Fabric
        - Databricks
        - Snowflake
        - PostgreSQL
        - MySQL
        - SQL Server
        - And many more...
    
    Example:
        >>> # For Synapse
        >>> engine = SynapseSQLEngine(settings, ComputeEnvironment.ETL)
        >>> df = engine.fetch_dataframe("SELECT * FROM table")
        >>> 
        >>> # For new platform (e.g., Snowflake)
        >>> class SnowflakeSQLEngine(BaseSQLEngine):
        ...     pass  # Just works with ODBC!
    """
    
    def __init__(self, settings: 'BaseComputeSettings', environment: ComputeEnvironment = ComputeEnvironment.ETL):
        """Initialize SQL engine.
        
        Args:
            settings: Platform settings inheriting from BaseComputeSettings
            environment: Compute environment (ETL or CONSUMPTION)
        """
        self.settings = settings  # Type: BaseComputeSettings (injected)
        self.environment: ComputeEnvironment = environment
        self._engine: Optional[Engine] = None
        self._connection_info: Dict[str, Any] = {
            "platform": self.__class__.__name__.replace("SQLEngine", "").lower(),
            "environment": environment.value
        }
    
    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine with lazy initialization.
        
        Returns:
            Engine: Configured SQLAlchemy engine
        """
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine
    
    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine with connection pooling.
        
        Returns:
            Engine: Configured SQLAlchemy engine
            
        Raises:
            ValueError: If no ODBC connection string is configured
            ConnectionError: If engine creation fails
        """
        try:
            # Disable pyodbc pooling as SQLAlchemy handles it
            pyodbc.pooling = False
            
            # Get ODBC connection string from settings
            odbc_str = self.settings.get_odbc_string(self.environment)
            if not odbc_str:
                raise ValueError(f"No ODBC connection string configured for {self.environment.value}")
            
            # URL encode the connection string
            params = parse.quote_plus(odbc_str)
            url = f"mssql+pyodbc:///?odbc_connect={params}"
            
            # Create engine with connection pool
            engine = create_engine(
                url,
                poolclass=QueuePool,
                pool_pre_ping=True,  # Verify connections before use
                pool_size=self.settings.sql_pool_size,
                max_overflow=self.settings.sql_max_overflow,
                pool_timeout=self.settings.sql_pool_timeout,
                connect_args={
                    "autocommit": True,  # Most platforms work better with autocommit
                }
            )
            
            platform = self._connection_info.get("platform", "SQL")
            logger.info(f"Created {platform} engine for {self.environment.value} environment")
            return engine
            
        except Exception as e:
            platform = self._connection_info.get("platform", "SQL")
            raise connection_error(
                f"Failed to connect to {platform}",
                service=platform,
                cause=e
            )
    
    @contextmanager
    def _get_connection(self):
        """Get a database connection from the pool.
        
        Yields:
            Connection: Database connection with platform-specific settings applied
        """
        conn = self.engine.connect()
        try:
            # Apply platform-specific connection settings
            self._apply_connection_settings(conn)
            yield conn
        finally:
            conn.close()
    
    def _apply_connection_settings(self, conn: Connection) -> None:
        """Apply platform-specific connection settings.
        
        Override this method in subclasses to apply platform-specific
        SET commands or other connection configuration.
        
        Args:
            conn: SQLAlchemy Connection object
        """
        pass 

    def _span_attributes(
        self,
        query: str,
        telemetry: Optional[Dict[str, str]] = None,
        *,
        operation: str,
        batch_position: Optional[int] = None,
        batch_total: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Build OpenTelemetry span attributes for SQL operations."""
        platform = self._connection_info.get("platform", "sql")
        sanitized_query = (query or "").strip()
        if sanitized_query and len(sanitized_query) > 4096:
            sanitized_query = f"{sanitized_query[:4093]}..."

        attributes: Dict[str, Any] = {
            "db.system": platform,
            "db.operation": operation,
            "medalflow.compute.environment": self.environment.value,
        }

        if sanitized_query:
            attributes["db.statement"] = sanitized_query
            attributes["db.statement.length"] = len(sanitized_query)

        if batch_position is not None:
            attributes["db.batch.index"] = batch_position
        if batch_total is not None:
            attributes["db.batch.count"] = batch_total

        if telemetry:
            table_name = telemetry.get("operation.object") or telemetry.get("operation.ctx.table")
            if table_name:
                attributes["db.sql.table"] = table_name
            for key, value in telemetry.items():
                attributes[f"medalflow.telemetry.{key}"] = value

        return attributes

    @traced(
        span_name="medalflow.compute.sql.execute",
        attribute_getter=lambda self, query, telemetry=None: self._span_attributes(
            query,
            telemetry,
            operation="execute",
        ),
    )
    @retry(max_retries=3, initial_delay=1, exponential_base=2)
    def execute_query(self, query: str, telemetry: Optional[Dict[str, str]] = None) -> None:
        """Execute a SQL query without returning results."""
        start_time = time.time()
        payload: Dict[str, str] = dict(telemetry or {})
        payload.setdefault("db.platform", str(self._connection_info.get("platform", "sql")))

        try:
            with self._get_connection() as conn:
                conn.execute(text(query))
                conn.commit()

            duration = time.time() - start_time
            logger.info(
                "SQL query executed",
                extra={**payload, "duration.seconds": f"{duration:.6f}"},
            )

        except Exception as exc:
            duration = time.time() - start_time
            logger.error(
                "SQL query failed",
                extra={**payload, "duration.seconds": f"{duration:.6f}", "error": str(exc)},
                exc_info=True,
            )
            raise query_execution_error(query, exc)
    
    @traced(
        span_name="medalflow.compute.sql.fetch_dataframe",
        attribute_getter=lambda self, query, telemetry=None: self._span_attributes(
            query,
            telemetry,
            operation="fetch_dataframe",
        ),
    )
    @retry(max_retries=3, initial_delay=1, exponential_base=2)
    def fetch_dataframe(self, query: str, telemetry: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """Execute query and return results as pandas DataFrame."""
        start_time = time.time()
        payload: Dict[str, str] = dict(telemetry or {})
        payload.setdefault("db.platform", str(self._connection_info.get("platform", "sql")))

        try:
            with self._get_connection() as conn:
                df = pd.read_sql(query, conn)

            duration = time.time() - start_time
            payload["rows"] = str(len(df))
            logger.info(
                "DataFrame fetched",
                extra={**payload, "duration.seconds": f"{duration:.6f}"},
            )
            return df

        except Exception as exc:
            duration = time.time() - start_time
            logger.error(
                "DataFrame fetch failed",
                extra={**payload, "duration.seconds": f"{duration:.6f}", "error": str(exc)},
                exc_info=True,
            )
            raise query_execution_error(query, exc)
    
    @traced(
        span_name="medalflow.compute.sql.fetch_scalar",
        attribute_getter=lambda self, query, telemetry=None: self._span_attributes(
            query,
            telemetry,
            operation="fetch_scalar",
        ),
    )
    @retry(max_retries=3, initial_delay=1, exponential_base=2)
    def fetch_scalar(self, query: str, telemetry: Optional[Dict[str, str]] = None) -> Any:
        """Execute query and return single scalar value.
        
        Used for queries that return a single value (COUNT, MAX, etc).
        
        Args:
            query: SQL query that returns single value
            telemetry: Optional context for logging/telemetry
            
        Returns:
            Single value from query result
            
        Raises:
            QueryExecutionError: If query execution fails
            ValueError: If query returns more than one value
        """
        start_time = time.time()
        payload: Dict[str, str] = dict(telemetry or {})
        payload.setdefault("db.platform", str(self._connection_info.get("platform", "sql")))
        
        try:
            with self._get_connection() as conn:
                result = conn.execute(text(query))
                
                # Use scalar_one_or_none for efficient single value retrieval
                value = result.scalar_one_or_none()
                
            duration = time.time() - start_time
            logger.info(
                "Scalar fetched",
                extra={**payload, "duration.seconds": f"{duration:.6f}", "value_is_null": str(value is None)},
            )
            return value
            
        except Exception as exc:
            duration = time.time() - start_time
            logger.error(
                "Scalar fetch failed",
                extra={**payload, "duration.seconds": f"{duration:.6f}", "error": str(exc)},
                exc_info=True,
            )
            raise query_execution_error(query, exc)
    
    @traced(
        span_name="medalflow.compute.sql.fetch_all",
        attribute_getter=lambda self, query, telemetry=None: self._span_attributes(
            query,
            telemetry,
            operation="fetch_all",
        ),
    )
    @retry(max_retries=3, initial_delay=1, exponential_base=2)
    def fetch_all(self, query: str, telemetry: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """Execute query and fetch all results as list of dictionaries."""
        start_time = time.time()
        payload: Dict[str, str] = dict(telemetry or {})
        payload.setdefault("db.platform", str(self._connection_info.get("platform", "sql")))
        
        try:
            with self._get_connection() as conn:
                result = conn.execute(text(query))
                rows = result.mappings().all()
                
            duration = time.time() - start_time
            payload["row_count"] = str(len(rows))
            logger.info(
                "Results fetched",
                extra={**payload, "duration.seconds": f"{duration:.6f}"},
            )
            return rows
            
        except Exception as exc:
            duration = time.time() - start_time
            logger.error(
                "Fetch all failed",
                extra={**payload, "duration.seconds": f"{duration:.6f}", "error": str(exc)},
                exc_info=True,
            )
            raise query_execution_error(query, exc)
    
    def test_connection(self) -> bool:
        """Test if connection to the engine is working.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            # Simple query to test connection
            result = self.fetch_scalar("SELECT 1 AS test")
            return result == 1
        except Exception as exc:
            platform = str(self._connection_info.get("platform", "SQL"))
            logger.error(
                "SQL connection test failed",
                extra={"db.platform": platform, "error": str(exc)},
                exc_info=True,
            )
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information for debugging/logging.
        
        Returns:
            Dictionary with connection details (server, database, etc.)
        """
        return self._connection_info.copy()
    
    @traced(
        span_name="medalflow.compute.sql.execute_batch",
        attribute_getter=lambda self, queries, telemetry=None: self._span_attributes(
            queries[0] if queries else "",
            telemetry,
            operation="execute_batch",
            batch_total=len(queries),
        ),
    )
    @retry(max_retries=3, initial_delay=1, exponential_base=2)
    def execute_batch(self, queries: List[str], telemetry: Optional[Dict[str, str]] = None) -> None:
        """Execute multiple queries in a batch."""
        start_time = time.time()
        payload: Dict[str, str] = dict(telemetry or {})
        payload.setdefault("db.platform", str(self._connection_info.get("platform", "sql")))
        
        try:
            with self._get_connection() as conn:
                total = len(queries)
                for index, query in enumerate(queries):
                    query_payload = {
                        **payload,
                        "batch.index": str(index),
                        "batch.total": str(total),
                    }
                    
                    try:
                        conn.execute(text(query))
                    except Exception as exc:
                        logger.error(
                            "Batch query failed",
                            extra={**query_payload, "error": str(exc)},
                            exc_info=True,
                        )
                        raise query_execution_error(query, exc)
                
                conn.commit()
                
            duration = time.time() - start_time
            logger.info(
                "Batch execution completed",
                extra={**payload, "duration.seconds": f"{duration:.6f}", "query_count": str(len(queries))},
            )
            
        except CTEError:
            raise
        except Exception as exc:
            logger.error(
                "Batch execution failed",
                extra={**payload, "error": str(exc)},
                exc_info=True,
            )
            raise query_execution_error("Batch execution failed", exc)
    
    def __del__(self):
        """Clean up engine on deletion."""
        if self._engine:
            self._engine.dispose()


class BaseSparkEngine(ABC):
    """Base interface for Spark execution engines.
    
    Simplified interface focusing on core Spark job execution capabilities.
    This interface defines the essential methods for submitting and managing
    Spark jobs across different platforms.
    
    Core Capabilities:
        - Submit Spark jobs with configuration
        - Monitor job status and progress
        - Retrieve job results or errors
        - Cancel running jobs
        - Validate Spark availability
    
    Implementation Notes:
        Unlike SQL engines which are synchronous, Spark engines typically
        operate asynchronously. Jobs are submitted and run in the background,
        with status polling for completion.
    
    Platform Support:
        - Azure Synapse Spark Pools
        - Microsoft Fabric Spark
        - Databricks Clusters
        - EMR Spark
        - Dataproc Spark
        
    Example:
        >>> config = SparkJobConfig(
        ...     name="data_processing",
        ...     main_file="process.py",
        ...     arguments=["--input", "raw", "--output", "processed"]
        ... )
        >>> job_id = engine.submit_job(config)
        >>> 
        >>> # Poll for completion
        >>> while True:
        ...     status = engine.get_job_status(job_id)
        ...     if status in [JobStatus.SUCCEEDED, JobStatus.FAILED]:
        ...         break
        ...     time.sleep(10)
    """
    
    # Default constants for Spark engines
    DEFAULT_SESSION_TIMEOUT = 1800          # 30 minutes - Idle timeout for sessions
    DEFAULT_STATEMENT_TIMEOUT = 600         # 10 minutes - Max time for statement execution
    DEFAULT_UPLOAD_CHUNK_SIZE = 4 * 1024 * 1024  # 4MB chunks for file uploads
    
    @abstractmethod
    def submit_job(self, config: SparkJobConfig) -> str:
        """Submit a Spark job for execution.
        
        Args:
            config: Spark job configuration
            
        Returns:
            Job ID for tracking
            
        Raises:
            SparkSubmissionError: If job submission fails
        """
        pass
    
    @abstractmethod
    def get_job_status(self, job_id: str) -> JobStatus:
        """Get current status of a Spark job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Current job status
            
        Raises:
            JobNotFoundError: If job doesn't exist
        """
        pass
    
    @abstractmethod
    def get_job_result(self, job_id: str) -> JobResult:
        """Get result of a completed Spark job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job execution result
            
        Raises:
            JobNotFoundError: If job doesn't exist
            JobNotCompleteError: If job is still running
        """
        pass
    
    @abstractmethod
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running Spark job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if cancellation succeeded
            
        Raises:
            JobNotFoundError: If job doesn't exist
        """
        pass
    
    @abstractmethod
    def is_spark_available(self) -> bool:
        """Check if Spark cluster is available.
        
        Returns:
            True if Spark is available and ready
        """
        pass
