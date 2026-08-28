"""Execution plan building for sequencers.

This module provides functionality to build execution plans in both
traditional (parallel/sequential) and DAG-based formats.
"""

from typing import Any

from medalflow.logging import get_logger
from medalflow.medallion.types import ExecutionPlan, ExecutionStage, LineageInfo
from medalflow.observability.context import sanitize_extras


class ExecutionPlanBuilder:
    """Builds execution plans from analyzed sequencer methods.

    This class creates structured execution plans that can be consumed by
    processors to execute queries using appropriate compute engines.

    Attributes:
        logger: Logger instance for this builder
        table_prefix: Optional table prefix for bronze tables
    """

    def __init__(self, table_prefix: str = ""):
        """Initialize the execution plan builder.

        Args:
            table_prefix: Optional prefix for table names (e.g., "cma_")
        """
        self.logger = get_logger(self.__class__.__name__)
        self.table_prefix = table_prefix

    def build_plan(
        self,
        stages: list[ExecutionStage],
        dag: dict[str, list[str]],
        lineage: dict[str, Any] | None,
        class_metadata: dict[str, Any],
        sequencer_name: str,
        total_queries: int,
    ) -> ExecutionPlan:
        """Build DAG-based execution plan with stages.

        Creates an execution plan organized into stages that can be
        executed in parallel within each stage.

        Args:
            stages: List of ExecutionStage objects from the DAG builder
            dag: Dependency graph
            lineage: Lineage information (None if disabled)
            class_metadata: Class-level metadata dictionary
            sequencer_name: Name of the sequencer class
            total_queries: Total number of queries in the plan

        Returns:
            DAG-based execution plan with stages and lineage
        """
        # Create LineageInfo object if lineage is provided
        lineage_info = LineageInfo(**lineage) if lineage else None

        # Create ExecutionPlan object
        execution_plan = ExecutionPlan(
            sequencer_name=sequencer_name,
            metadata=class_metadata,
            stages=stages,
            dependency_graph=dag,
            lineage=lineage_info,
            total_queries=total_queries,
        )

        self.logger.info(
            "execution_plan.created",
            extra=sanitize_extras(
                {
                    "sequencer": sequencer_name,
                    "num_stages": len(stages),
                    "total_queries": total_queries,
                }
            ),
        )

        return execution_plan

    def validate_plan(self, execution_plan: ExecutionPlan) -> bool:
        """Validate an execution plan for completeness and correctness.

        Args:
            execution_plan: The execution plan to validate

        Returns:
            True if plan is valid

        Raises:
            ValueError: If plan is invalid with details about the issue
        """
        # Check required fields (these are guaranteed by Pydantic but we can still validate)
        if not execution_plan.sequencer_name:
            raise ValueError("Execution plan missing sequencer_name")

        if execution_plan.total_queries < 0:
            raise ValueError(f"Invalid total_queries: {execution_plan.total_queries}")

        # get_all_operations() groups operations by stage, so flatten before
        # counting — len() of the grouped result is the stage count.
        all_operations = [
            operation
            for stage_operations in execution_plan.get_all_operations()
            for operation in stage_operations
        ]
        actual_count = len(all_operations)

        # Validate query count
        if actual_count != execution_plan.total_queries:
            raise ValueError(
                f"Query count mismatch: expected {execution_plan.total_queries}, "
                f"found {actual_count}"
            )

        # Validate each operation has required fields
        for operation in all_operations:
            # For BaseOperation, we check schema_name and object_name instead of
            # SQL. Note `operation.schema` resolves to pydantic's deprecated
            # BaseModel.schema() classmethod, which is always truthy, so this
            # check silently never fired.
            if not operation.schema_name or not operation.object_name:
                method_name = getattr(operation, "method", "unknown")
                raise ValueError(f"Operation missing required fields: {method_name}")

        self.logger.debug(
            "execution_plan.validated",
            extra=sanitize_extras(
                {
                    "total_queries": execution_plan.total_queries,
                    "actual_queries": actual_count,
                }
            ),
        )
        return True
