from typing import Any

from medalflow.medallion import (
    ExecutionPlan,
    ExecutionPlanOrchestrator,
    IntrospectedBronzeSequencer,
)
from medalflow.medallion.bronze.metadata_discovery import BronzeMetadataDiscovery
from medalflow.medallion.gold.metadata_discovery import GoldMetadataDiscovery
from medalflow.medallion.silver.metadata_discovery import SilverMetadataDiscovery
from medalflow.observability.context import execution_request_scope, resolve_request_context
from medalflow.settings import get_settings


def _attach_plan_context(plan: ExecutionPlan, ctx) -> ExecutionPlan:
    plan.attach_context(ctx)
    return plan


def _instantiate_sequencers(transformations, settings) -> list[Any]:
    """Turn discovered transformation metadata into sequencer instances.

    Discovery yields `TransformationMetadata` dataclasses, but the orchestrator
    calls `get_obj_name()`, `get_queries()` and `_get_class_metadata()` on what
    it receives. Passing the metadata through raised AttributeError on the very
    first line of its loop.
    """
    return [transformation.sequencer_class(settings) for transformation in transformations]


def _bronze_sequencers(settings, table_names: list[str] | None) -> list[Any]:
    """The bronze sequencers this deployment's configuration asks for.

    Declared models are the default; introspection is opted into with
    `bronze_introspection`. The mode is read off configuration and never
    inferred from whether discovery found anything: a mistyped
    `MEDALFLOW_BRONZE_PACKAGE` would otherwise fall back to querying a
    warehouse, turning a loud authoring error into a silent one.

    Args:
        settings: Application settings
        table_names: Optional selection, forwarded to each sequencer

    Returns:
        One sequencer per declared bronze model, or a single introspecting
        sequencer over the whole source schema
    """
    if settings.bronze_introspection:
        return [IntrospectedBronzeSequencer(settings, table_names)]

    # No `is_model_configured` gate here, deliberately: that setting is
    # `configured_models`, silver's grouping concept. Bronze models declare
    # no `model=`, so gating bronze on it would drop every one of them --
    # the same reasoning that keeps the gate off gold.
    metadata_discovery = BronzeMetadataDiscovery(settings.package_for_layer("bronze"))

    return [
        model.sequencer_class(settings, table_names) for model in metadata_discovery.discover_all()
    ]


def get_bronze_execution_plan(
    table_names: list[str] | None,
    *,
    ctx: Any | None = None,
) -> ExecutionPlan:
    """Generate the execution plan for the bronze layer."""
    context = resolve_request_context(ctx)
    with execution_request_scope(context, operation="medalflow.medallion.plan.bronze"):
        settings = get_settings()
        plan_orchestrator = ExecutionPlanOrchestrator(settings)
        plan = plan_orchestrator.create_plan_for_bronze_layer(
            bronze_sequencers=_bronze_sequencers(settings, table_names)
        )
        return _attach_plan_context(plan, context)


def get_gold_execution_plan(
    table_names: list[str] | None,
    *,
    ctx: Any | None = None,
) -> ExecutionPlan:
    """Generate the execution plan for the gold layer."""
    context = resolve_request_context(ctx)
    with execution_request_scope(context, operation="medalflow.medallion.plan.gold"):
        settings = get_settings()
        plan_orchestrator = ExecutionPlanOrchestrator(settings)
        # No `is_model_configured` gate here, deliberately: that setting is
        # `configured_models`, silver's grouping concept. Gold models declare
        # no `model=`, so gating gold on it would drop every one of them.
        metadata_discovery = GoldMetadataDiscovery(settings.package_for_layer("gold"))
        models = metadata_discovery.discover_all()
        plan = plan_orchestrator.create_plan_for_gold_layer(
            gold_sequencers=[model.sequencer_class(settings, table_names) for model in models]
        )
        return _attach_plan_context(plan, context)


def get_silver_execution_plan_for_models(
    models: str = "all",
    *,
    ctx: Any | None = None,
) -> ExecutionPlan:
    """Generate the execution plan for the silver layer."""
    context = resolve_request_context(ctx)
    with execution_request_scope(context, operation="medalflow.medallion.plan.silver.models"):
        settings = get_settings()
        plan_orchestrator = ExecutionPlanOrchestrator(settings)
        metadata_discovery = SilverMetadataDiscovery(settings.package_for_layer("silver"))
        transformations = metadata_discovery.get_transformations_by_models(models=models)
        plan = plan_orchestrator.create_plan_for_silver_layer(
            silver_sequencers=_instantiate_sequencers(transformations, settings)
        )
        return _attach_plan_context(plan, context)


def get_execution_plan_for_sps(
    sp_names: str,
    *,
    ctx: Any | None = None,
) -> ExecutionPlan:
    """Generate the execution plan for specific stored procedures."""
    context = resolve_request_context(ctx)
    with execution_request_scope(context, operation="medalflow.medallion.plan.silver.sps"):
        settings = get_settings()
        plan_orchestrator = ExecutionPlanOrchestrator(settings)
        metadata_discovery = SilverMetadataDiscovery(settings.package_for_layer("silver"))
        transformations = metadata_discovery.get_transformations_by_names(names=sp_names)
        plan = plan_orchestrator.create_plan_for_silver_layer(
            silver_sequencers=_instantiate_sequencers(transformations, settings)
        )
        return _attach_plan_context(plan, context)
