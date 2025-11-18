from typing import Any, Dict, Optional

from core.compute import get_platform_factory, ComputeEnvironment, OperationResult
from core.monitoring.metrics import MetricsCollector
from core.observability import operation_instrumentation
from core.observability.context import resolve_request_context
from core.settings import get_settings


_metrics_collector: Optional[MetricsCollector] = None




def _get_metrics() -> MetricsCollector:
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector(get_settings())
    return _metrics_collector


def execute(
    operation: dict,
    compute_environment: ComputeEnvironment.ETL,
    *,
    ctx: Optional[Dict[str, Any]] = None,
) -> OperationResult:
    """Execute a database operation using the configured platform.

    Args:
        operation: Serialized operation dictionary.
        compute_environment: Optional override (enum or string). If omitted, the
            value is resolved from the serialized operation/context attributes.
        ctx: Optional request context dictionary carrying logging/trace metadata.
    """
    platform = get_platform_factory().create(compute_environment)
    ctx = resolve_request_context(ctx)
    stage = str(operation.get("_cte_stage", "unknown"))
    op_name = str(operation.get("operation_type", "unknown"))

    def _stringify(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return str(value)
        return str(value)

    attributes: Dict[str, str] = {}
    for key, raw in {
        "schema": operation.get("schema_name"),
        "object": operation.get("object_name"),
        "operation_type": op_name,
        "compute_environment": getattr(compute_environment, "value", compute_environment),
    }.items():
        sanitized = _stringify(raw)
        if sanitized:
            attributes[key] = sanitized

    with operation_instrumentation(
        ctx,
        stage_name=stage,
        operation_name=op_name,
        metrics=_get_metrics(),
        attributes=attributes,
    ) as telemetry:
        return platform.execute(operation, telemetry=telemetry)


def test_connection(compute_env: ComputeEnvironment = ComputeEnvironment.ETL) -> bool:
    """Test connectivity to the configured compute platform."""
    platform = get_platform_factory().create(compute_env)
    return platform.test_connection()

