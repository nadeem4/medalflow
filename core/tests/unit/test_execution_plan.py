"""Regression tests for ExecutionPlan construction (Phase 1, task 6).

No plan has ever validated. `ExecutionPlan.lineage` was a required
non-Optional `LineageInfo`, but every caller passes `lineage=None`
(`execution_orchestrator.py:110`, `base/sequencer.py:114`). `metadata` was a
required `ClassMetadata` union, but the orchestrator passes a plain dict.
Before that, stage creation read `operation.schema` and the plan builder
`setattr`-ed undeclared fields onto pydantic models.
"""

import logging

import pytest

from medalflow.constants.sql import QueryType
from medalflow.medallion.orchestration.execution_orchestrator import ExecutionPlanOrchestrator
from medalflow.medallion.types import ExecutionPlan
from medalflow.medallion.utils.execution_plan_builder import ExecutionPlanBuilder
from medalflow.operations import Select
from medalflow.types.metadata import SQLDependencies


class _StubAnalyzer:
    """Returns a precomputed dependency map.

    The real analyzer calls `create_query_builder()`, which needs live
    warehouse settings. Dependency extraction itself is covered by
    test_dependency_graph.py; this keeps plan assembly offline per D6.
    """

    def __init__(self, dependencies):
        self._dependencies = dependencies

    def analyze_operations(self, operations):
        return self._dependencies


def _select(schema, name):
    return Select(
        operation_type=QueryType.SELECT,
        schema_name=schema,
        object_name=name,
        columns=["*"],
    )


@pytest.fixture
def orchestrator_factory():
    def build(dependencies):
        orchestrator = ExecutionPlanOrchestrator.__new__(ExecutionPlanOrchestrator)
        orchestrator.settings = None
        orchestrator.sql_analyzer = _StubAnalyzer(dependencies)
        orchestrator.plan_builder = ExecutionPlanBuilder()
        orchestrator.logger = logging.getLogger("test-orchestrator")
        return orchestrator

    return build


@pytest.fixture
def two_dependent_operations():
    bronze = _select("bronze", "Customers")
    silver = _select("silver", "DimCustomer")
    dependencies = {
        bronze: SQLDependencies(reads_from=set(), writes_to="bronze.customers"),
        silver: SQLDependencies(
            reads_from={"bronze.customers"}, writes_to="silver.dimcustomer"
        ),
    }
    return bronze, silver, dependencies


def test_orchestrator_builds_plan_from_two_dependent_operations(
    orchestrator_factory, two_dependent_operations
):
    bronze, silver, dependencies = two_dependent_operations

    plan = orchestrator_factory(dependencies).create_execution_plan(
        operations=[bronze, silver], sequencer_name="DimCustomerModel"
    )

    assert isinstance(plan, ExecutionPlan)
    assert plan.sequencer_name == "DimCustomerModel"
    assert plan.total_queries == 2

    # The dependent operation must land in a later stage than its source.
    assert len(plan.stages) == 2
    assert [op.object_name for op in plan.stages[0].operations] == ["Customers"]
    assert [op.object_name for op in plan.stages[1].operations] == ["DimCustomer"]

    assert plan.dependency_graph[silver._dag_id] == [bronze._dag_id]


def test_plan_lineage_and_metadata_default_to_none(
    orchestrator_factory, two_dependent_operations
):
    bronze, silver, dependencies = two_dependent_operations

    plan = orchestrator_factory(dependencies).create_execution_plan(
        operations=[bronze, silver]
    )

    assert plan.lineage is None


def test_plan_accepts_orchestrator_metadata_dict(
    orchestrator_factory, two_dependent_operations
):
    """The orchestrator passes a plain dict, not a layer-metadata model."""
    bronze, silver, dependencies = two_dependent_operations

    plan = orchestrator_factory(dependencies).create_execution_plan(
        operations=[bronze, silver],
        metadata={"sequencer_metadata": {}, "sequencers": ["DimCustomerModel"]},
    )

    assert plan.metadata["sequencers"] == ["DimCustomerModel"]


def test_building_a_plan_does_not_mutate_operations(
    orchestrator_factory, two_dependent_operations
):
    """`setattr(operation, 'dependencies', ...)` raised on a pydantic model."""
    bronze, silver, dependencies = two_dependent_operations

    orchestrator_factory(dependencies).create_execution_plan(
        operations=[bronze, silver]
    )

    assert not hasattr(bronze, "layer")
    assert not hasattr(bronze, "dependencies")


def test_validate_plan_counts_operations_not_stages(
    orchestrator_factory, two_dependent_operations
):
    """`get_all_operations()` returns a list per stage, so len() was the stage count."""
    bronze, silver, dependencies = two_dependent_operations
    orchestrator = orchestrator_factory(dependencies)

    plan = orchestrator.create_execution_plan(operations=[bronze, silver])

    assert orchestrator.plan_builder.validate_plan(plan) is True


def test_validate_plan_rejects_a_wrong_query_count(
    orchestrator_factory, two_dependent_operations
):
    bronze, silver, dependencies = two_dependent_operations
    orchestrator = orchestrator_factory(dependencies)
    plan = orchestrator.create_execution_plan(operations=[bronze, silver])
    plan.total_queries = 99

    with pytest.raises(ValueError, match="Query count mismatch"):
        orchestrator.plan_builder.validate_plan(plan)
