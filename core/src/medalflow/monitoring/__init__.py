"""Monitoring infrastructure for metrics and telemetry.

This module provides metrics collection, performance tracking, and
integration with Azure Application Insights and OpenTelemetry.
"""

from medalflow.monitoring.metrics import MetricsCollector

__all__ = [
    "MetricsCollector",
]