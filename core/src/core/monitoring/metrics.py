"""Metrics collection for ETL operations.

This module provides classes for collecting and exporting metrics
related to ETL operations, performance, and resource usage.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING

from opentelemetry.metrics import CallbackOptions, Observation

from core.logging import get_logger
from core.telemetry import get_meter
from core.__version__ import __version__

if TYPE_CHECKING:
    from core.settings.main import _Settings as SettingsType
else:
    SettingsType = Any


@dataclass
class ETLMetrics:
    """Container for ETL operation metrics.
    
    This dataclass holds metrics data for a single ETL operation,
    including timing, row counts, and success/failure information.
    
    Attributes:
        operation: Type of operation (e.g., 'create_table', 'execute_query')
        layer: Data layer (bronze, silver, gold)
        table_name: Name of the table being processed
        rows_processed: Number of rows processed
        duration_seconds: Operation duration in seconds
        success: Whether the operation succeeded
        error_message: Error message if operation failed
        timestamp: When the operation occurred
        engine_type: Type of engine used
        query_type: Type of query executed
        bytes_processed: Bytes of data processed (optional)
    """
    
    operation: str
    layer: str
    table_name: str
    rows_processed: int
    duration_seconds: float
    success: bool
    error_message: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    engine_type: Optional[str] = None
    query_type: Optional[str] = None
    bytes_processed: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        data = asdict(self)
        # Convert datetime to ISO format
        data['timestamp'] = self.timestamp.isoformat()
        return data


@dataclass
class PerformanceMetrics:
    """Container for performance-related metrics.
    
    Attributes:
        cpu_percent: CPU usage percentage
        memory_mb: Memory usage in MB
        disk_io_mb: Disk I/O in MB
        network_io_mb: Network I/O in MB
        active_connections: Number of active database connections
        timestamp: When metrics were collected
    """
    
    cpu_percent: float
    memory_mb: float
    disk_io_mb: float
    network_io_mb: float
    active_connections: int
    timestamp: datetime = field(default_factory=datetime.utcnow)


class MetricsCollector:
    """Collector for ETL and performance metrics.
    
    This class collects metrics and exports them to OpenTelemetry
    for monitoring and analysis.
    
    Attributes:
        settings: Application settings
        logger: Logger instance
        meter: OpenTelemetry meter
        _metrics: List of collected metrics
        _performance_metrics: List of performance metrics
    """
    
    def __init__(self, settings: SettingsType):
        """Initialize metrics collector.
        
        Args:
            settings: Application settings
        """
        self.settings = settings
        self.logger = get_logger(__name__)
        self._metrics: List[ETLMetrics] = []
        self._performance_metrics: List[PerformanceMetrics] = []
        
        # Initialize OpenTelemetry meter
        observability = getattr(self.settings, "observability", None)
        meter_name = getattr(observability, "service_name", "medalflow")
        meter_version = getattr(observability, "service_version", None) or __version__
        self.meter = get_meter(meter_name, meter_version)
        self._setup_instruments()
    
    def _setup_instruments(self) -> None:
        """Setup OpenTelemetry instruments."""
        # Counters
        self.stage_counter = self.meter.create_counter(
            "etl_stages_total",
            description="Total number of ETL stages",
            unit="stages"
        )

        self.operation_counter = self.meter.create_counter(
            "etl_operations_total",
            description="Total number of ETL operations",
            unit="operations"
        )
        
        self.rows_counter = self.meter.create_counter(
            "etl_rows_processed_total",
            description="Total rows processed",
            unit="rows"
        )
        
        self.bytes_counter = self.meter.create_counter(
            "etl_bytes_processed_total",
            description="Total bytes processed",
            unit="bytes"
        )
        
        self.error_counter = self.meter.create_counter(
            "etl_errors_total",
            description="Total number of errors",
            unit="errors"
        )
        
        # Histograms
        self.duration_histogram = self.meter.create_histogram(
            "etl_operation_duration_seconds",
            description="Duration of ETL operations",
            unit="seconds"
        )
        
        self.rows_histogram = self.meter.create_histogram(
            "etl_rows_per_operation",
            description="Rows processed per operation",
            unit="rows"
        )
        
        # Gauges (via callbacks)
        self.meter.create_observable_gauge(
            "etl_active_operations",
            callbacks=[self._active_operations_callback],
            description="Currently active ETL operations",
            unit="operations"
        )
        
        self.meter.create_observable_gauge(
            "etl_success_rate",
            callbacks=[self._success_rate_callback],
            description="ETL operation success rate",
            unit="ratio"
        )
        
        self.meter.create_observable_gauge(
            "system_cpu_percent",
            callbacks=[self._cpu_usage_callback],
            description="System CPU usage",
            unit="percent"
        )
        
        self.meter.create_observable_gauge(
            "system_memory_mb",
            callbacks=[self._memory_usage_callback],
            description="System memory usage",
            unit="megabytes"
        )
    
    def record_etl_operation(self, metrics: ETLMetrics) -> None:
        """Record an ETL operation.
        
        Args:
            metrics: ETL metrics to record
        """
        # Store metrics
        self._metrics.append(metrics)
        
        # Create attributes for OpenTelemetry
        attributes = {
            "operation": metrics.operation,
            "layer": metrics.layer,
            "table": metrics.table_name,
            "success": str(metrics.success).lower(),
            "engine_type": metrics.engine_type or "unknown",
            "query_type": metrics.query_type or "unknown"
        }
        
        # Update counters
        self.operation_counter.add(1, attributes)
        self.rows_counter.add(metrics.rows_processed, attributes)
        
        if metrics.bytes_processed:
            self.bytes_counter.add(metrics.bytes_processed, attributes)
        
        if not metrics.success:
            error_attributes = attributes.copy()
            error_attributes["error_type"] = type(metrics.error_message).__name__ if metrics.error_message else "unknown"
            self.error_counter.add(1, error_attributes)
        
        # Update histograms
        self.duration_histogram.record(metrics.duration_seconds, attributes)
        self.rows_histogram.record(metrics.rows_processed, attributes)
        
        # Log metrics
        self.logger.info(
            "ETL operation recorded",
            **metrics.to_dict()
        )
    
    def record_performance_metrics(self, metrics: PerformanceMetrics) -> None:
        """Record system performance metrics.
        
        Args:
            metrics: Performance metrics to record
        """
        self._performance_metrics.append(metrics)
        
        # Log if concerning levels
        if metrics.cpu_percent > 80:
            self.logger.warning(
                "High CPU usage detected",
                cpu_percent=metrics.cpu_percent
            )
        
        if metrics.memory_mb > 1024 * 8:  # 8GB
            self.logger.warning(
                "High memory usage detected",
                memory_mb=metrics.memory_mb
            )
    
    def _active_operations_callback(self, options: CallbackOptions) -> Iterable[Observation]:
        """Callback for active operations gauge."""
        # Count operations started in last 5 minutes that haven't completed
        cutoff = datetime.utcnow() - timedelta(minutes=5)
        active = sum(
            1 for m in self._metrics
            if m.timestamp > cutoff and not m.success and m.error_message is None
        )
        
        yield Observation(
            active,
            {"environment": self.settings.data_source.environment}
        )
    
    def _success_rate_callback(self, options: CallbackOptions) -> Iterable[Observation]:
        """Callback for success rate gauge."""
        # Calculate success rate for last hour
        cutoff = datetime.utcnow() - timedelta(hours=1)
        recent_metrics = [m for m in self._metrics if m.timestamp > cutoff]
        
        if recent_metrics:
            success_count = sum(1 for m in recent_metrics if m.success)
            success_rate = success_count / len(recent_metrics)
        else:
            success_rate = 1.0
        
        yield Observation(
            success_rate,
            {"environment": self.settings.data_source.environment}
        )
    
    def _cpu_usage_callback(self, options: CallbackOptions) -> Iterable[Observation]:
        """Callback for CPU usage gauge."""
        if self._performance_metrics:
            latest = self._performance_metrics[-1]
            yield Observation(
                latest.cpu_percent,
                {"environment": self.settings.data_source.environment}
            )
    
    def _memory_usage_callback(self, options: CallbackOptions) -> Iterable[Observation]:
        """Callback for memory usage gauge."""
        if self._performance_metrics:
            latest = self._performance_metrics[-1]
            yield Observation(
                latest.memory_mb,
                {"environment": self.settings.data_source.environment}
            )
    
    def get_metrics_summary(self, time_window: timedelta = timedelta(hours=1)) -> Dict[str, Any]:
        """Get summary of metrics for a time window.
        
        Args:
            time_window: Time window to summarize
            
        Returns:
            Dictionary with metrics summary
        """
        cutoff = datetime.utcnow() - time_window
        recent_metrics = [m for m in self._metrics if m.timestamp > cutoff]
        
        if not recent_metrics:
            return {
                "total_operations": 0,
                "successful_operations": 0,
                "failed_operations": 0,
                "success_rate": 0.0,
                "total_rows": 0,
                "total_duration_seconds": 0.0,
                "average_duration_seconds": 0.0
            }
        
        successful = [m for m in recent_metrics if m.success]
        failed = [m for m in recent_metrics if not m.success]
        
        return {
            "total_operations": len(recent_metrics),
            "successful_operations": len(successful),
            "failed_operations": len(failed),
            "success_rate": len(successful) / len(recent_metrics),
            "total_rows": sum(m.rows_processed for m in recent_metrics),
            "total_duration_seconds": sum(m.duration_seconds for m in recent_metrics),
            "average_duration_seconds": sum(m.duration_seconds for m in recent_metrics) / len(recent_metrics),
            "operations_by_layer": self._group_by_attribute(recent_metrics, "layer"),
            "operations_by_type": self._group_by_attribute(recent_metrics, "operation"),
            "errors_by_type": self._group_errors(failed)
        }
    
    def _group_by_attribute(
        self,
        metrics_list: List[ETLMetrics],
        attribute: str
    ) -> Dict[str, int]:
        """Group metrics by an attribute."""
        grouped = {}
        for metric in metrics_list:
            value = getattr(metric, attribute)
            grouped[value] = grouped.get(value, 0) + 1
        return grouped
    
    def _group_errors(self, failed_metrics: List[ETLMetrics]) -> Dict[str, int]:
        """Group errors by type."""
        errors = {}
        for metric in failed_metrics:
            error_type = "unknown"
            if metric.error_message:
                # Try to extract error type from message
                if "timeout" in metric.error_message.lower():
                    error_type = "timeout"
                elif "connection" in metric.error_message.lower():
                    error_type = "connection"
                elif "permission" in metric.error_message.lower():
                    error_type = "permission"
                else:
                    error_type = "other"
            
            errors[error_type] = errors.get(error_type, 0) + 1
        
        return errors
    
    def clear_old_metrics(self, retention_days: int = 7) -> None:
        """Clear metrics older than retention period.
        
        Args:
            retention_days: Number of days to retain metrics
        """
        cutoff = datetime.utcnow() - timedelta(days=retention_days)
        
        # Clear old ETL metrics
        self._metrics = [m for m in self._metrics if m.timestamp > cutoff]
        
        # Clear old performance metrics
        self._performance_metrics = [
            m for m in self._performance_metrics if m.timestamp > cutoff
        ]
        
        self.logger.info(
            "Cleared old metrics",
            retention_days=retention_days,
            remaining_etl_metrics=len(self._metrics),
            remaining_perf_metrics=len(self._performance_metrics)
        )
