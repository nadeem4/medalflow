"""Shared observability context utilities."""

from __future__ import annotations

import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from pydantic import Field

from medalflow.logging import get_logger
from medalflow.logging.filters import clear_request_context, set_request_context
from medalflow.types.base import CTEBaseModel


class ExecutionRequestContext(CTEBaseModel):
    """Observability context propagated across an execution request."""

    request_id: str
    user_id: str | None = None
    correlation_id: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)
    telemetry_base: dict[str, str] = Field(default_factory=dict, exclude=True)

    @classmethod
    def generate(cls, **kwargs: Any) -> ExecutionRequestContext:
        """Generate a new context with a unique request id."""
        ctx = cls(request_id=str(uuid.uuid4()), **kwargs)
        ctx.telemetry_base = ctx.to_telemetry_dict()
        return ctx

    @staticmethod
    def _stringify(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str | int | float | bool):
            return str(value)
        return str(value)

    def to_telemetry_dict(self) -> dict[str, str]:
        """Flatten the context into the string-only dict telemetry wants.

        Span attributes and log fields are strings, so everything is coerced
        and anything that coerces to nothing is dropped rather than emitted
        as ``"None"``. Caller-supplied ``attributes`` are namespaced under
        ``ctx.`` so they cannot collide with ``request_id`` and its siblings.
        """
        payload: dict[str, str] = {"request_id": self.request_id}
        if self.user_id:
            payload["user_id"] = self.user_id
        if self.correlation_id:
            payload["correlation_id"] = self.correlation_id
        for key, value in (self.attributes or {}).items():
            sanitized = self._stringify(value)
            if sanitized is not None:
                payload[f"ctx.{key}"] = sanitized
        return payload


@contextmanager
def execution_request_scope(
    ctx: ExecutionRequestContext,
    *,
    operation: str | None = None,
) -> Iterator[trace.Span]:
    """Apply logging + tracing scope for a request/operation."""
    ctx.telemetry_base = ctx.to_telemetry_dict()

    set_request_context(request_id=ctx.request_id, user_id=ctx.user_id)

    tracer = trace.get_tracer("medalflow")
    span_name = operation or "medalflow.request"
    span_attributes = {f"medalflow.{key}": value for key, value in ctx.telemetry_base.items()}
    if operation:
        span_attributes["medalflow.operation.name"] = operation

    with tracer.start_as_current_span(span_name) as span:
        for key, value in span_attributes.items():
            span.set_attribute(key, value)

        try:
            yield span
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR))
            get_logger(__name__).error(
                "Execution failed",
                extra={**ctx.telemetry_base, "operation.name": operation or "medalflow.request"},
                exc_info=True,
            )
            raise
        finally:
            clear_request_context()


def resolve_request_context(ctx: Any | None) -> ExecutionRequestContext:
    """Normalize inbound context data into an ExecutionRequestContext."""
    if isinstance(ctx, ExecutionRequestContext):
        if not ctx.telemetry_base:
            ctx.telemetry_base = ctx.to_telemetry_dict()
        return ctx

    if ctx is None:
        return ExecutionRequestContext.generate()

    if isinstance(ctx, str):
        return ExecutionRequestContext(request_id=ctx)

    data: dict[str, Any] = {}

    if isinstance(ctx, Mapping):
        data = dict(ctx)
    else:
        # Attempt attribute lookups (for Durable function context objects, etc.)
        for key in ("request_id", "id", "instance_id"):
            if hasattr(ctx, key):
                data["request_id"] = getattr(ctx, key)
                break
        for key in ("user_id", "correlation_id", "traceparent", "attributes"):
            if hasattr(ctx, key):
                data[key] = getattr(ctx, key)

    request_id = data.get("request_id") or data.get("id") or data.get("instance_id")
    if not request_id:
        request_id = str(uuid.uuid4())
    else:
        request_id = str(request_id)

    attributes = data.get("attributes") or {}
    if not isinstance(attributes, dict):
        attributes = {"value": str(attributes)}

    ctx_obj = ExecutionRequestContext(
        request_id=request_id,
        user_id=data.get("user_id"),
        correlation_id=data.get("correlation_id") or data.get("traceparent"),
        attributes=attributes,
    )
    ctx_obj.telemetry_base = ctx_obj.to_telemetry_dict()
    return ctx_obj


def sanitize_extras(
    extra: dict[str, Any] | None,
    *,
    prefix: str | None = None,
) -> dict[str, str]:
    """Sanitize arbitrary telemetry extras into a JSON-safe dict."""
    if not extra:
        return {}

    result: dict[str, str] = {}
    for key, value in extra.items():
        sanitized = ExecutionRequestContext._stringify(value)
        if sanitized is None:
            continue
        field = f"{prefix}{key}" if prefix else str(key)
        result[field] = sanitized
    return result


def merge_telemetry(
    ctx: ExecutionRequestContext,
    *,
    extra: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Merge request context telemetry with additional key/value pairs."""
    if not ctx.telemetry_base:
        ctx.telemetry_base = ctx.to_telemetry_dict()

    payload = dict(ctx.telemetry_base)
    if extra:
        payload.update(sanitize_extras(extra))
    return payload
