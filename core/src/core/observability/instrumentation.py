"""Context managers for per-stage and per-operation instrumentation."""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

from opentelemetry.trace import Status, StatusCode

from core.logging import get_logger
from core.monitoring.metrics import MetricsCollector
from core.observability.context import (
    ExecutionRequestContext,
    execution_request_scope,
    merge_telemetry,
    sanitize_extras,
)
from core.telemetry import get_tracer

logger = get_logger(__name__)


def _build_tags(
    *,
    ctx: ExecutionRequestContext,
    stage: Optional[str],
    operation: Optional[str],
    extra: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    tags: Dict[str, str] = {
        "request_id": ctx.request_id,
    }
    if stage:
        tags["stage"] = stage
    if operation:
        tags["operation"] = operation
    if ctx.correlation_id:
        tags["correlation_id"] = ctx.correlation_id
    if extra:
        tags.update(extra)
    return tags



@contextmanager
def operation_instrumentation(
    ctx: ExecutionRequestContext,
    *,
    stage_name: str,
    operation_name: str,
    metrics: MetricsCollector,
    attributes: Optional[Dict[str, str]] = None,
) -> Iterator[Dict[str, str]]:
    """Instrument a single operation."""
    tags = _build_tags(
        ctx=ctx,
        stage=stage_name,
        operation=operation_name,
        extra=attributes,
    )
    telemetry_payload = merge_telemetry(
        ctx,
        extra={"operation.stage": stage_name, "operation.name": operation_name},
    )
    telemetry_payload.update(sanitize_extras(attributes))

    start_time = time.perf_counter()
    with execution_request_scope(ctx, operation=f"medalflow.operation.{operation_name}"):
        tracer = get_tracer("medalflow")
        with tracer.start_as_current_span(f"medalflow.operation.{operation_name}") as span:
            for key, value in telemetry_payload.items():
                span.set_attribute(f"medalflow.{key}", value)
            try:
                yield telemetry_payload
                elapsed = time.perf_counter() - start_time
                metrics.operation_counter.add(1, {**tags, "status": "success"})
                metrics.duration_histogram.record(elapsed, {**tags, "scope": "operation"})
            except Exception as exc:
                elapsed = time.perf_counter() - start_time
                metrics.operation_counter.add(1, {**tags, "status": "error"})
                metrics.duration_histogram.record(elapsed, {**tags, "scope": "operation", "status": "error"})
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR))
                logger.error(
                    "Operation failed",
                    extra=telemetry_payload,
                    exc_info=True,
                )
                raise
