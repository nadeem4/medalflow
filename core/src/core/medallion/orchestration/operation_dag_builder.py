"""DAG builder for database operations.

This module provides the OperationDAGBuilder class that builds
dependency graphs from collections of database operations.
"""

from typing import Dict, List, Optional, Any, TYPE_CHECKING

from core.logging import get_logger
from core.observability.context import sanitize_extras
from core.medallion.types import DependencyDAG
from core.types.metadata import SQLDependencies
from core.operations import BaseOperation

if TYPE_CHECKING:
    from core.settings import _Settings

logger = get_logger(__name__)


class OperationDAGBuilder:
    """Builds dependency DAGs from database operations.
    
    This class constructs directed acyclic graphs (DAGs) representing
    dependencies between database operations based on their input/output
    relationships.
    
    Attributes:
        operations: List of operations to build DAG from
        dependencies: Dependency information for each operation
        dag: The dependency DAG being built
        operation_map: Maps operation IDs to operations
        table_to_operation: Maps output tables to operations that create them
    """
    
    def __init__(
        self,
        operations: List[BaseOperation],
        dependencies: Dict[BaseOperation, SQLDependencies],
        settings: "_Settings"
    ):
        """Initialize the operation DAG builder.
        
        Args:
            operations: List of operations to build DAG from
            dependencies: SQL dependency information for each operation
            settings: Application settings
        """
        self.operations = operations
        self.dependencies = dependencies
        self.settings = settings
        self.logger = logger
        
        # Initialize DAG
        self.dag = DependencyDAG()
        
        # Create operation tracking maps
        self.operation_map: Dict[str, BaseOperation] = {}
        self.table_to_operation: Dict[str, str] = {}
        
        # Initialize operation identifiers
        self._initialize_operation_ids()
    
    def _initialize_operation_ids(self) -> None:
        """Initialize unique identifiers for operations."""
        for i, op in enumerate(self.operations):
            # Create a unique ID for each operation
            # Use table name if available, otherwise use index
            if hasattr(op, 'object_name') and op.object_name:
                op_id = f"{op.schema_name}.{op.object_name}_{i}"
            else:
                op_id = f"operation_{i}"
            
            self.operation_map[op_id] = op
            # Store the ID on the operation for reference
            op._dag_id = op_id
    
    def build_dag(self) -> DependencyDAG:
        """Build dependency DAG from operations.
        
        Creates a directed acyclic graph where:
        - Nodes are operations
        - Edges represent dependencies (A -> B means B depends on A)
        
        Returns:
            DependencyDAG representing operation dependencies
            
        Raises:
            ValueError: If dependency analysis is incomplete
        """
        self.logger.debug(
            "dag.build.start",
            extra=sanitize_extras({"operation_count": len(self.operations)}),
        )
        
        # 1. Create table-to-operation mapping
        self._create_table_to_operation_mapping()
        
        # 2. Build the dependency graph
        self._create_dependency_graph()
        
        return self.dag
    
    def _create_table_to_operation_mapping(self) -> None:
        """Map output tables to the operations that create them."""
        for op in self.operations:
            if op not in self.dependencies:
                self.logger.warning(
                    "dag.missing_dependency_info",
                    extra=sanitize_extras({"operation": repr(op)}),
                )
                continue
            
            dep_info = self.dependencies[op]
            if dep_info.writes_to:
                # Map the output table to this operation
                self.table_to_operation[dep_info.writes_to] = op._dag_id
                self.logger.debug(
                    "dag.register_table_mapping",
                    extra=sanitize_extras(
                        {"table": dep_info.writes_to, "operation_id": op._dag_id}
                    ),
                )
    
    def _create_dependency_graph(self) -> None:
        """Create the dependency graph by analyzing data flow between operations."""
        for op in self.operations:
            op_id = op._dag_id
            operation_dependencies = []
            
            if op not in self.dependencies:
                # Operation has no dependencies, add as standalone node
                self.dag.add_node(op_id)
                continue
            
            dep_info = self.dependencies[op]
            
            # Find dependencies based on tables this operation reads
            for source_table in dep_info.reads_from:
                if source_table in self.table_to_operation:
                    dep_op_id = self.table_to_operation[source_table]
                    if dep_op_id != op_id:  # Avoid self-dependencies
                        operation_dependencies.append(dep_op_id)
                        self.logger.debug(
                            "dag.dependency.detected",
                            extra=sanitize_extras(
                                {
                                    "operation_id": op_id,
                                    "dependency_id": dep_op_id,
                                    "table": source_table,
                                }
                            ),
                        )
            
            # Add node with dependencies
            if operation_dependencies:
                self.dag.add_edges(op_id, operation_dependencies)
            else:
                self.dag.add_node(op_id)
    
    def validate_dag(self, dag: Optional[DependencyDAG] = None) -> None:
        """Validate DAG for cycles.
        
        Args:
            dag: DAG to validate (uses self.dag if not provided)
            
        Raises:
            ValueError: If a cycle is detected in the DAG
        """
        dag = dag or self.dag
        
        if dag.has_cycles():
            raise ValueError(
                "Circular dependency detected in operations DAG. "
                "Please check your operations for circular table dependencies."
            )
        
        adjacency_list = dag.get_adjacency_list()
        self.logger.debug(
            "dag.validate.success",
            extra=sanitize_extras({"node_count": len(adjacency_list)}),
        )
    
    def create_execution_stages(self) -> List[Dict[str, Any]]:
        """Create execution stages from the DAG.
        
        Uses topological sorting to group operations into stages where:
        - All operations in a stage can run in parallel
        - Stages must be executed sequentially
        
        Returns:
            List of execution stages with operation details
            
        Raises:
            ValueError: If DAG has cycles or cannot be sorted
        """
        try:
            node_stages = self.dag.get_execution_stages()
        except ValueError as e:
            raise ValueError(f"Failed to create execution stages: {e}")
        
        # Convert node stages to operation stages
        execution_stages = []
        for stage_num, stage_nodes in enumerate(node_stages, 1):
            stage_operations = []
            
            for node_id in stage_nodes:
                if node_id not in self.operation_map:
                    self.logger.warning(
                        "dag.missing_operation_mapping",
                        extra=sanitize_extras({"node_id": node_id}),
                    )
                    continue
                
                operation = self.operation_map[node_id]
                
                # Create stage operation info
                stage_op_info = {
                    'operation': operation,
                    'id': node_id,
                    'dependencies': self.dag.get_dependencies(node_id),
                    'layer': operation.schema,
                    'logging_context': operation.logging_context,
                    'operation_type': operation.operation_type
                }
                
                stage_operations.append(stage_op_info)
            
            if stage_operations:
                execution_stages.append({
                    'stage': stage_num,
                    'parallel_queries': stage_operations
                })
                self.logger.debug(
                    "dag.stage.created",
                    extra=sanitize_extras(
                        {"stage": stage_num, "operation_count": len(stage_operations)}
                    ),
                )
        
        return execution_stages
    
    def get_operation_dependencies(self, operation: BaseOperation) -> List[BaseOperation]:
        """Get the operations that a given operation depends on.
        
        Args:
            operation: The operation to get dependencies for
            
        Returns:
            List of operations that must complete before this operation
        """
        if not hasattr(operation, '_dag_id'):
            return []
        
        dep_ids = self.dag.get_dependencies(operation._dag_id)
        return [self.operation_map[dep_id] for dep_id in dep_ids if dep_id in self.operation_map]
    
    def get_dependent_operations(self, operation: BaseOperation) -> List[BaseOperation]:
        """Get the operations that depend on a given operation.
        
        Args:
            operation: The operation to get dependents for
            
        Returns:
            List of operations that depend on this operation
        """
        if not hasattr(operation, '_dag_id'):
            return []
        
        adjacency_list = self.dag.get_adjacency_list()
        dependents = []
        
        for node_id, deps in adjacency_list.items():
            if operation._dag_id in deps:
                if node_id in self.operation_map:
                    dependents.append(self.operation_map[node_id])
        
        return dependents
