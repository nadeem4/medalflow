"""OpenTelemetry helpers for instrumentation."""

from typing import Optional

from opentelemetry import metrics, trace

__all__ = [
    "get_tracer",
    "get_meter",
]


def get_tracer(name: str, version: Optional[str] = None):
    """Return a tracer from the active OpenTelemetry provider."""
    return trace.get_tracer(name, version)


def get_meter(name: str, version: Optional[str] = None):
    """Return a meter from the active OpenTelemetry provider."""
    return metrics.get_meter(name, version)
