"""OpenTelemetry instruments for ETL operations.

Holds the instruments `core.observability.instrumentation` records against.
MedalFlow depends on the OpenTelemetry *API* only — if the host application
has not configured a MeterProvider, these instruments are no-ops and nothing
is exported (Decision D5).
"""

from typing import Any, TYPE_CHECKING

from opentelemetry import metrics as otel_metrics

from core.__version__ import __version__

if TYPE_CHECKING:
    from core.settings.main import _Settings as SettingsType
else:
    SettingsType = Any


class MetricsCollector:
    """Creates and holds the OpenTelemetry instruments used by MedalFlow."""

    def __init__(self, settings: SettingsType):
        """Initialize the collector.

        Args:
            settings: Application settings. Retained for future use; the meter
                itself is named after the package.
        """
        self.settings = settings
        self.meter = otel_metrics.get_meter("medalflow", __version__)

        self.operation_counter = self.meter.create_counter(
            "etl_operations_total",
            description="Total number of ETL operations",
            unit="operations",
        )
        self.duration_histogram = self.meter.create_histogram(
            "etl_operation_duration_seconds",
            description="Duration of ETL operations",
            unit="seconds",
        )
