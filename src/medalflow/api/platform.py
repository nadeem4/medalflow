"""``execute()`` -- one serialized operation, run now.

The seam for callers that bring their own orchestration. ``run()`` builds a
plan and walks it; this takes a single operation dict of the kind
``ExecutionPlan.get_all_operations(serialize=True)`` produces, which is what
lets an Airflow or ADF task be one operation without MedalFlow scheduling
anything. ``run()`` is itself a loop over this function, so the two paths
cannot drift.

Both functions build a platform per call, from settings. The metrics
collector is the one thing held across calls: constructing it reads settings,
and there is nothing per-operation about it.
"""

from typing import Any

from medalflow.compute import ComputeEnvironment, OperationResult, create_platform
from medalflow.monitoring.metrics import MetricsCollector
from medalflow.observability import operation_instrumentation
from medalflow.observability.context import ExecutionRequestContext, resolve_request_context
from medalflow.settings import get_settings

_metrics_collector: MetricsCollector | None = None


def _get_metrics() -> MetricsCollector:
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector(get_settings())
    return _metrics_collector


def execute(
    operation: dict,
    compute_environment: ComputeEnvironment = ComputeEnvironment.ETL,
    *,
    ctx: dict[str, Any] | None = None,
) -> OperationResult:
    """Execute a database operation using the configured platform.

    Args:
        operation: Serialized operation dictionary.
        compute_environment: Optional override (enum or string). If omitted, the
            value is resolved from the serialized operation/context attributes.
        ctx: Optional request context dictionary carrying logging/trace metadata.
    """
    platform = create_platform(compute_environment)
    ctx = resolve_request_context(ctx)
    stage = str(operation.get("_cte_stage", "unknown"))
    op_name = str(operation.get("operation_type", "unknown"))

    attributes: dict[str, str] = {}
    for key, raw in {
        "schema": operation.get("schema_name"),
        "object": operation.get("object_name"),
        "operation_type": op_name,
        "compute_environment": getattr(compute_environment, "value", compute_environment),
    }.items():
        sanitized = ExecutionRequestContext._stringify(raw)
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


def test_connection(
    compute_env: ComputeEnvironment = ComputeEnvironment.ETL,
) -> dict[str, bool]:
    """Test connectivity to the configured compute platform.

    Args:
        compute_env: Which compute environment to reach

    Returns:
        One entry per engine the platform supports, e.g. ``{"sql": True}``.
        It was annotated ``bool``, which the value never was -- and a dict is
        truthy either way, so a caller trusting the annotation read a failed
        connection test as a passing one.
    """
    platform = create_platform(compute_env)
    return platform.test_connection()
