"""SQLAlchemy-backed SQL execution.

``pyodbc`` (the DBAPI driver) and ``pandas`` (the DataFrame fetch path) are
imported inside the two methods that need them: both ship with the optional
``azure`` extra, and importing this module must not require it. SQLAlchemy
itself is a hard dependency and stays at module scope.
"""

import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional
from urllib import parse

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import QueuePool

from medalflow.common.exceptions import CTEError, connection_error, query_execution_error
from medalflow.common.optional_deps import require_module
from medalflow.constants.compute import ComputeEnvironment
from medalflow.logging import get_logger
from medalflow.utils.decorators import retry_with_backoff as retry
from medalflow.utils.decorators import traced

if TYPE_CHECKING:
    import pandas as pd

    from medalflow.settings import ComputeSettings

logger = get_logger(__name__)


class BaseSQLEngine:
    """SQLAlchemy-based SQL execution engine for all platforms.

    This concrete implementation provides full SQL engine functionality using
    SQLAlchemy, which supports Synapse and many
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

    def __init__(
        self,
        settings: "ComputeSettings",
        environment: ComputeEnvironment = ComputeEnvironment.ETL,
    ):
        """Initialize SQL engine.

        Args:
            settings: Compute platform settings
            environment: Compute environment (ETL or CONSUMPTION)
        """
        self.settings = settings  # Type: ComputeSettings (injected)
        self.environment: ComputeEnvironment = environment
        self._engine: Optional[Engine] = None
        self._connection_info: dict[str, Any] = {
            "platform": self.__class__.__name__.replace("SQLEngine", "").lower(),
            "environment": environment.value,
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
            CTEError: If pyodbc is not installed, or if engine creation fails
            ValueError: If no ODBC connection string is configured
        """
        # Outside the try: a missing driver is not a connection failure, and
        # wrapping it would bury the install instruction under one.
        # Disable pyodbc pooling as SQLAlchemy handles it.
        require_module("pyodbc").pooling = False

        try:
            # Get ODBC connection string from settings
            odbc_str = self.settings.get_odbc_string(self.environment)
            if not odbc_str:
                raise ValueError(
                    f"No ODBC connection string configured for {self.environment.value}"
                )

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
                },
            )

            platform = self._connection_info.get("platform", "SQL")
            logger.info(f"Created {platform} engine for {self.environment.value} environment")
            return engine

        except Exception as e:
            platform = self._connection_info.get("platform", "SQL")
            raise connection_error(
                f"Failed to connect to {platform}", service=platform, cause=e
            ) from e

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
        telemetry: Optional[dict[str, str]] = None,
        *,
        operation: str,
        batch_position: Optional[int] = None,
        batch_total: Optional[int] = None,
    ) -> dict[str, Any]:
        """Build OpenTelemetry span attributes for SQL operations."""
        platform = self._connection_info.get("platform", "sql")
        sanitized_query = (query or "").strip()
        if sanitized_query and len(sanitized_query) > 4096:
            sanitized_query = f"{sanitized_query[:4093]}..."

        attributes: dict[str, Any] = {
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
    def execute_query(self, query: str, telemetry: Optional[dict[str, str]] = None) -> None:
        """Execute a SQL query without returning results."""
        start_time = time.time()
        payload: dict[str, str] = dict(telemetry or {})
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
            raise query_execution_error(query, exc) from exc

    @traced(
        span_name="medalflow.compute.sql.fetch_dataframe",
        attribute_getter=lambda self, query, telemetry=None: self._span_attributes(
            query,
            telemetry,
            operation="fetch_dataframe",
        ),
    )
    @retry(max_retries=3, initial_delay=1, exponential_base=2)
    def fetch_dataframe(
        self, query: str, telemetry: Optional[dict[str, str]] = None
    ) -> "pd.DataFrame":
        """Execute query and return results as pandas DataFrame."""
        pd = require_module("pandas")
        start_time = time.time()
        payload: dict[str, str] = dict(telemetry or {})
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
            raise query_execution_error(query, exc) from exc

    @traced(
        span_name="medalflow.compute.sql.fetch_scalar",
        attribute_getter=lambda self, query, telemetry=None: self._span_attributes(
            query,
            telemetry,
            operation="fetch_scalar",
        ),
    )
    @retry(max_retries=3, initial_delay=1, exponential_base=2)
    def fetch_scalar(self, query: str, telemetry: Optional[dict[str, str]] = None) -> Any:
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
        payload: dict[str, str] = dict(telemetry or {})
        payload.setdefault("db.platform", str(self._connection_info.get("platform", "sql")))

        try:
            with self._get_connection() as conn:
                result = conn.execute(text(query))

                # Use scalar_one_or_none for efficient single value retrieval
                value = result.scalar_one_or_none()

            duration = time.time() - start_time
            logger.info(
                "Scalar fetched",
                extra={
                    **payload,
                    "duration.seconds": f"{duration:.6f}",
                    "value_is_null": str(value is None),
                },
            )
            return value

        except Exception as exc:
            duration = time.time() - start_time
            logger.error(
                "Scalar fetch failed",
                extra={**payload, "duration.seconds": f"{duration:.6f}", "error": str(exc)},
                exc_info=True,
            )
            raise query_execution_error(query, exc) from exc

    @traced(
        span_name="medalflow.compute.sql.fetch_all",
        attribute_getter=lambda self, query, telemetry=None: self._span_attributes(
            query,
            telemetry,
            operation="fetch_all",
        ),
    )
    @retry(max_retries=3, initial_delay=1, exponential_base=2)
    def fetch_all(
        self, query: str, telemetry: Optional[dict[str, str]] = None
    ) -> list[dict[str, Any]]:
        """Execute query and fetch all results as list of dictionaries."""
        start_time = time.time()
        payload: dict[str, str] = dict(telemetry or {})
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
            raise query_execution_error(query, exc) from exc

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

    def get_connection_info(self) -> dict[str, Any]:
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
    def execute_batch(self, queries: list[str], telemetry: Optional[dict[str, str]] = None) -> None:
        """Execute multiple queries in a batch."""
        start_time = time.time()
        payload: dict[str, str] = dict(telemetry or {})
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
                        raise query_execution_error(query, exc) from exc

                conn.commit()

            duration = time.time() - start_time
            logger.info(
                "Batch execution completed",
                extra={
                    **payload,
                    "duration.seconds": f"{duration:.6f}",
                    "query_count": str(len(queries)),
                },
            )

        except CTEError:
            raise
        except Exception as exc:
            logger.error(
                "Batch execution failed",
                extra={**payload, "error": str(exc)},
                exc_info=True,
            )
            raise query_execution_error("Batch execution failed", exc) from exc

    def __del__(self):
        """Clean up engine on deletion."""
        if self._engine:
            self._engine.dispose()
