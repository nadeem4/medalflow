"""Execution plan building for sequencers.

This module provides functionality to build execution plans in both
traditional (parallel/sequential) and DAG-based formats.
"""

from typing import Any, Dict, List, Optional

from core.medallion.types import (
    ExecutionStage,
    LineageInfo,
    ExecutionPlan
)
from core.operations.dml import Select
from core.logging import get_logger
from core.observability.context import sanitize_extras


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
    
    def build_plan(self,
                      stages: List[Dict[str, Any]],
                      dag: Dict[str, List[str]],
                      lineage: Optional[Dict[str, Any]],
                      class_metadata: Dict[str, Any],
                      sequencer_name: str,
                      total_queries: int) -> ExecutionPlan:
        """Build DAG-based execution plan with stages.
        
        Creates an execution plan organized into stages that can be
        executed in parallel within each stage.
        
        Args:
            stages: List of execution stages from DAG builder
            dag: Dependency graph
            lineage: Lineage information (None if disabled)
            class_metadata: Class-level metadata dictionary
            sequencer_name: Name of the sequencer class
            total_queries: Total number of queries in the plan
            
        Returns:
            DAG-based execution plan with stages and lineage
        """
        # Convert stage dicts to ExecutionStage objects with BaseOperation objects
        execution_stages = []
        for stage_dict in stages:
            operations = []
            for query_dict in stage_dict.get('parallel_queries', []):
                # Operation is directly provided
                operation = query_dict['operation']
                # Add any additional metadata as attributes
                for key, value in query_dict.items():
                    if key not in ['operation', 'id'] and not hasattr(operation, key):
                        setattr(operation, key, value)
                operations.append(operation)
            
            execution_stages.append(ExecutionStage(
                stage=stage_dict['stage'],
                operations=operations
            ))
        
        # Create LineageInfo object if lineage is provided
        lineage_info = LineageInfo(**lineage) if lineage else None
        
        # Create ExecutionPlan object
        execution_plan = ExecutionPlan(
            sequencer_name=sequencer_name,
            metadata=class_metadata,
            stages=execution_stages,
            dependency_graph=dag,
            lineage=lineage_info,
            total_queries=total_queries
        )
        
        self.logger.info(
            'execution_plan.created',
            extra=sanitize_extras({
                'sequencer': sequencer_name,
                'num_stages': len(stages),
                'total_queries': total_queries,
            }),
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
        
        # Get all operations using the helper method
        all_operations = execution_plan.get_all_operations()
        actual_count = len(all_operations)
        
        # Validate query count
        if actual_count != execution_plan.total_queries:
            raise ValueError(
                f"Query count mismatch: expected {execution_plan.total_queries}, "
                f"found {actual_count}"
            )
        
        # Validate each operation has required fields
        for operation in all_operations:
            # For BaseOperation, we check schema and object_name instead of SQL
            if not operation.schema or not operation.object_name:
                method_name = getattr(operation, 'method', 'unknown')
                raise ValueError(f"Operation missing required fields: {method_name}")
        
        self.logger.debug(
            'execution_plan.validated',
            extra=sanitize_extras({
                'total_queries': execution_plan.total_queries,
                'actual_queries': actual_count,
            }),
        )
        return True
    
    