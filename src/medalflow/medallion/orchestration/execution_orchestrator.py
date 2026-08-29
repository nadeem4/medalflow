"""Generic execution plan orchestrator for database operations.

This module provides the ExecutionPlanOrchestrator class that creates
execution plans from any collection of database operations, regardless
of their source (single sequencer, multiple transformations, or custom lists).

The orchestrator:
- Analyzes dependencies between operations
- Builds optimal execution DAGs
- Creates parallel execution stages
- Supports operations from multiple sources
"""

from typing import TYPE_CHECKING, Any

from medalflow.logging import get_logger
from medalflow.medallion.types import ExecutionPlan
from medalflow.medallion.utils.execution_plan_builder import ExecutionPlanBuilder
from medalflow.medallion.utils.sql_dependency_analyzer import SQLDependencyAnalyzer
from medalflow.observability.context import sanitize_extras
from medalflow.operations import BaseOperation

from .operation_dag_builder import OperationDAGBuilder

if TYPE_CHECKING:
    from medalflow.medallion.base.sequencer import _BaseSequencer
    from medalflow.settings import MedalflowSettings

logger = get_logger(__name__)


class ExecutionPlanOrchestrator:
    """Orchestrates execution plan creation from collections of operations.

    This class provides a unified interface for creating execution plans
    from any collection of BaseOperation instances. It handles dependency
    analysis, DAG building, and stage creation independently of the
    operations' source.

    Attributes:
        settings: Application settings
        sql_analyzer: SQL dependency analyzer
        dag_builder: Operation DAG builder
        plan_builder: Execution plan builder
    """

    def __init__(self, settings: "MedalflowSettings"):
        """Initialize the execution plan orchestrator.

        Args:
            settings: Application settings containing configuration
        """
        self.settings = settings
        self.sql_analyzer = SQLDependencyAnalyzer(settings)
        self.plan_builder = ExecutionPlanBuilder()
        self.logger = logger

    def create_execution_plan(
        self,
        operations: list[BaseOperation],
        metadata: dict[str, Any] | None = None,
        sequencer_name: str | None = None,
    ) -> ExecutionPlan:
        """Create an execution plan from a list of operations.

        This method analyzes dependencies between operations and creates
        an optimal execution plan with parallel stages where possible.

        Args:
            operations: List of database operations to plan
            metadata: Optional metadata about the operations' source
            sequencer_name: Optional name of the source sequencer

        Returns:
            ExecutionPlan with optimized execution stages

        Raises:
            ValueError: If operations list is empty or invalid
            RuntimeError: If circular dependencies are detected
        """
        if not operations:
            raise ValueError("Cannot create execution plan from empty operations list")

        self.logger.info(
            "orchestrator.plan.create",
            extra=sanitize_extras(
                {
                    "operation_count": len(operations),
                    "sequencer": sequencer_name or "unknown",
                }
            ),
        )

        operation_dependencies = self.sql_analyzer.analyze_operations(operations)

        dag_builder = OperationDAGBuilder(
            operations=operations, dependencies=operation_dependencies, settings=self.settings
        )
        dag = dag_builder.build_dag()
        dag_builder.validate_dag(dag)

        stages = dag_builder.create_execution_stages()

        lineage = None

        return self.plan_builder.build_plan(
            stages=stages,
            dag=dag.get_adjacency_list(),
            lineage=lineage,
            class_metadata=metadata or {},
            sequencer_name=sequencer_name or "ExecutionPlanOrchestrator",
            total_queries=len(operations),
        )

    def create_plan_from_sequencers(self, sequencers: list["_BaseSequencer"]) -> ExecutionPlan:
        """Create a combined execution plan from multiple sequencers.

        This method combines operations from multiple sequencers into a
        unified execution plan that respects dependencies across all
        transformations.

        Args:
            sequencers: List of sequencer instances to combine

        Returns:
            Combined ExecutionPlan for all sequencers

        Example:
            >>> orchestrator = ExecutionPlanOrchestrator(settings)
            >>> sequencers = [DimCustomerSeq(), DimProductSeq(), FactSalesSeq()]
            >>> plan = orchestrator.create_plan_from_sequencers(sequencers)
        """
        if not sequencers:
            raise ValueError("Cannot create plan from empty sequencer list")

        all_metadata = {}
        operations = []

        for sequencer in sequencers:
            seq_name = sequencer.get_obj_name()
            try:
                operations.extend(sequencer.get_queries())
                all_metadata[seq_name] = sequencer._get_class_metadata()
            except Exception as e:
                # Fail the whole plan. Skipping the sequencer would quietly
                # shrink the plan to whichever models happened to work, which
                # is the silent degradation this phase removes.
                self.logger.error(
                    "orchestrator.sequencer_get_queries_failed",
                    extra=sanitize_extras(
                        {"sequencer": seq_name, "error": str(e)},
                    ),
                    exc_info=True,
                )
                raise ValueError(
                    f"Cannot build execution plan: sequencer '{seq_name}' failed: {e}"
                ) from e

        self.logger.info(
            "orchestrator.plan.create_from_sequencers",
            extra=sanitize_extras(
                {
                    "sequencer_count": len(sequencers),
                    "metadata_keys": list(all_metadata.keys()),
                }
            ),
        )

        return self.create_execution_plan(
            operations=operations,
            metadata={
                "sequencer_metadata": all_metadata,
                "sequencers": [s.get_obj_name() for s in sequencers],
            },
        )
