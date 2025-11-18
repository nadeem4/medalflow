from typing import Any, List, Optional

from core.medallion import (
    ExecutionPlan,
    ExecutionPlanOrchestrator,
    BronzeSequencer,
    GoldSequencer,
    SilverMetadataDiscovery,
)
from core.observability.context import execution_request_scope, resolve_request_context
from core.settings import get_settings


def _attach_plan_context(plan: ExecutionPlan, ctx) -> ExecutionPlan:
    plan.attach_context(ctx)
    return plan


def get_bronze_execution_plan(
    table_names: Optional[List[str]],
    *,
    ctx: Optional[Any] = None,
) -> ExecutionPlan:
    """Generate the execution plan for the bronze layer."""
    context = resolve_request_context(ctx)
    with execution_request_scope(context, operation="medalflow.medallion.plan.bronze"):
        settings = get_settings()
        plan_orchestrator = ExecutionPlanOrchestrator(settings)
        plan = plan_orchestrator.create_plan_for_bronze_layer(
            bronze_sequencer=BronzeSequencer(table_names=table_names)
        )
        return _attach_plan_context(plan, context)


def get_gold_execution_plan(
    table_names: Optional[List[str]],
    *,
    ctx: Optional[Any] = None,
) -> ExecutionPlan:
    """Generate the execution plan for the gold layer."""
    context = resolve_request_context(ctx)
    with execution_request_scope(context, operation="medalflow.medallion.plan.gold"):
        settings = get_settings()
        plan_orchestrator = ExecutionPlanOrchestrator(settings)
        plan = plan_orchestrator.create_plan_for_gold_layer(
            gold_sequencer=GoldSequencer(selected_tables=table_names)
        )
        return _attach_plan_context(plan, context)


def get_silver_execution_plan_for_models(
    models: str = "all",
    *,
    ctx: Optional[Any] = None,
) -> ExecutionPlan:
    """Generate the execution plan for the silver layer."""
    context = resolve_request_context(ctx)
    with execution_request_scope(context, operation="medalflow.medallion.plan.silver.models"):
        settings = get_settings()
        plan_orchestrator = ExecutionPlanOrchestrator(settings)
        metadata_discovery = SilverMetadataDiscovery(settings.silver_package_name)
        silver_sequencers = metadata_discovery.get_transformations_by_models(models=models)
        plan = plan_orchestrator.create_plan_for_silver_layer(
            silver_sequencers=silver_sequencers
        )
        return _attach_plan_context(plan, context)


def get_execution_plan_for_sps(
    sp_names: str,
    *,
    ctx: Optional[Any] = None,
) -> ExecutionPlan:
    """Generate the execution plan for specific stored procedures."""
    context = resolve_request_context(ctx)
    with execution_request_scope(context, operation="medalflow.medallion.plan.silver.sps"):
        settings = get_settings()
        plan_orchestrator = ExecutionPlanOrchestrator(settings)
        metadata_discovery = SilverMetadataDiscovery(settings.silver_package_name)
        silver_sequencers = metadata_discovery.get_transformation_by_sp(sp_names=sp_names)
        plan = plan_orchestrator.create_plan_for_silver_layer(
            silver_sequencers=silver_sequencers
        )
        return _attach_plan_context(plan, context)
