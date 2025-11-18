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

from typing import Dict, List, Optional, Any, TYPE_CHECKING

from core.logging import get_logger
from core.observability.context import sanitize_extras
from core.medallion.types import ExecutionPlan, ExecutionStage, LineageInfo
from core.medallion.utils.sql_dependency_analyzer import SQLDependencyAnalyzer
from core.medallion.utils.execution_plan_builder import ExecutionPlanBuilder
from core.operations import BaseOperation
from .operation_dag_builder import OperationDAGBuilder
from core.constants import Layer

if TYPE_CHECKING:
    from core.settings import _Settings
    from core.medallion.base.sequencer import _BaseSequencer
    from core.medallion.bronze.sequencer import BronzeSequencer
    from core.medallion.gold.sequencer import GoldSequencer
    from core.medallion.silver.sequencer import SilverTransformationSequencer

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
    
    def __init__(self, settings: "_Settings"):
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
        operations: List[BaseOperation],
        metadata: Optional[Dict[str, Any]] = None,
        sequencer_name: Optional[str] = None
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
            operations=operations,
            dependencies=operation_dependencies,
            settings=self.settings
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
            total_queries=len(operations)
        )
    
    def create_plan_from_sequencers(
        self, 
        sequencers: List["_BaseSequencer"]  
    ) -> ExecutionPlan:
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
                self.logger.warning(
                    "orchestrator.sequencer_get_queries_failed",
                    extra=sanitize_extras(
                        {"sequencer": seq_name, "error": str(e)},
                    ),
                    exc_info=True,
                )
                continue


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
                'sequencer_metadata': all_metadata,
                'sequencers': [s.get_obj_name() for s in sequencers]
            }
        )
    

    def create_plan_for_bronze_layer(
        self,
        bronze_sequencer: "BronzeSequencer"
    ) -> ExecutionPlan:
        """Create an execution plan specifically for a bronze layer sequencer.
        
        This method generates an execution plan tailored for bronze layer
        operations, ensuring that all necessary metadata and lineage
        information is included.
        
        Args:
            sequencer: The bronze layer sequencer instance
        Returns:
            ExecutionPlan for the bronze layer operations
        """

        return self.create_plan_from_sequencers([bronze_sequencer])
    
    def create_plan_for_gold_layer(
        self,
        gold_sequencer: "GoldSequencer"
    ) -> ExecutionPlan:
        """Create an execution plan specifically for a gold layer sequencer.
        
        This method generates an execution plan tailored for gold layer
        operations, ensuring that all necessary metadata and lineage
        information is included.
        
        Args:
            sequencer: The gold layer sequencer instance    
        Returns:

            ExecutionPlan for the gold layer operations
        """

        return self.create_plan_from_sequencers([gold_sequencer])
    

    def create_plan_for_silver_layer(
        self,
        silver_sequencers: List["SilverTransformationSequencer"]
    ) -> ExecutionPlan:
        """Create an execution plan specifically for a silver layer sequencer.
        
        This method generates an execution plan tailored for silver layer
        operations, ensuring that all necessary metadata and lineage
        information is included.
        
        Args:
            sequencer: The silver layer sequencer instance
        Returns:

            ExecutionPlan for the silver layer operations
        """

        return self.create_plan_from_sequencers(silver_sequencers)
    
        
    
    
    
    
    def optimize_plan(self, plan: ExecutionPlan) -> ExecutionPlan:
        """Optimize an execution plan for better performance.
        
        This method applies various optimizations to an execution plan:
        - Merges compatible operations in the same stage
        - Reorders operations within stages for better cache usage
        - Identifies operations that can use the same connection
        
        Args:
            plan: The execution plan to optimize
            
        Returns:
            Optimized execution plan
        """
        # Future optimization implementation
        # For now, return the plan as-is
        return plan
