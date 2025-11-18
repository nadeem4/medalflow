"""Medallion architecture type definitions.

This module contains all type definitions for the medallion architecture,
including execution plans, DAGs, lineage tracking, and database metadata.
"""

from collections import defaultdict, deque
from typing import Any, Dict, List, Set, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from core.types.base import CTEBaseModel
from core.operations import BaseOperation
from core.types.metadata import ClassMetadata
from core.observability.context import ExecutionRequestContext



class TableInfo(BaseModel):
    """Information about a database table.
    
    This model represents metadata for a single table in the database,
    including its name, schema, and fully qualified identifier.
    
    Attributes:
        table_name: The name of the table without schema qualification.
            Example: "customer_orders", "product_catalog".
        schema_name: The database schema containing the table.
            Example: "dbo", "staging", "analytics".
        full_table_name: The fully qualified table name combining schema and table.
            Format: "schema.table_name". Example: "dbo.customer_orders".
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    table_name: str = Field(
        ...,
        description="Name of the table without schema qualification"
    )
    schema_name: str = Field(
        ...,
        description="Database schema containing the table"
    )
    full_table_name: str = Field(
        ...,
        description="Fully qualified table name (schema.table_name)"
    )
    
    def __str__(self) -> str:
        """String representation of the table info."""
        return self.full_table_name
    
    def __repr__(self) -> str:
        """Developer-friendly representation of the table info."""
        return f"TableInfo(table='{self.table_name}', schema='{self.schema_name}')"




class LineageInfo(CTEBaseModel):
    """Basic lineage information for ETL pipelines.
    
    This type provides a simple structure for tracking lineage information
    in ETL pipelines.
    
    Attributes:
        lineage_data: Dictionary containing lineage information
    """
    lineage_data: Dict[str, Any] = Field(default_factory=dict)



class ExecutionStage(CTEBaseModel):
    """A stage in DAG execution containing parallel operations.
    
    In DAG-based execution, operations are organized into stages where all
    operations within a stage can be executed in parallel. Stages must be
    executed sequentially in order.
    
    Attributes:
        stage: Stage number (1-based) indicating execution order
        operations: List of operations that can run in parallel
    """
    stage: int
    operations: List[BaseOperation]
    context: Optional[ExecutionRequestContext] = None
    
    def to_dict(self) -> dict:
        """Override to ensure operations are properly serialized."""
        return {
            "stage": self.stage,
            "operations": [op.to_dict() for op in self.operations],
            "context": self.context.model_dump() if self.context else None,
        }

    def attach_context(
        self,
        ctx: ExecutionRequestContext,
    ) -> None:
        """Attach context to the stage and contained operations."""
        self.context = ctx
        for _, operation in enumerate(self.operations):
            operation.attach_context(
                ctx
            )


class ExecutionPlan(CTEBaseModel):
    """Base class for all execution plans.
    
    Common structure for both traditional and DAG-based execution plans,
    containing metadata, lineage, and query information.
    
    Attributes:
        sequencer_name: Name of the sequencer class that generated this plan
        metadata: Class-level metadata from layer decorators
        lineage: Complete lineage information for the plan
        total_queries: Total number of queries in the plan
    """
    sequencer_name: str
    metadata: ClassMetadata  
    lineage: LineageInfo
    total_queries: int
    stages: List[ExecutionStage]  
    dependency_graph: Dict[str, List[str]]
    context: Optional[ExecutionRequestContext] = None
    
    def to_dict(self) -> dict:
        """Override to ensure stages and nested objects are properly serialized."""
        return {
            "sequencer_name": self.sequencer_name,
            "metadata": self.metadata.to_dict() if self.metadata else None,
            "lineage": self.lineage.to_dict() if self.lineage else None,
            "total_queries": self.total_queries,
            "stages": [stage.to_dict() for stage in self.stages],
            "dependency_graph": self.dependency_graph,
            "context": self.context.model_dump() if self.context else None,
        }  

    def get_all_operations(self, serialize: bool = False) -> Union[List[List[BaseOperation]], List[List[dict]]]:
        """Get all operations grouped by execution stage.
        
        Operations within the same stage (inner list) can be executed in parallel.
        Stages must be executed sequentially in order.
        
        Args:
            serialize: If True, return as List[List[dict]] (serialized for API)
                      If False, return as List[List[BaseOperation]] (for direct execution)
        
        Returns:
            - List[List[BaseOperation]] if serialize=False - operations grouped by stage
            - List[List[dict]] if serialize=True - serialized operations grouped by stage
        """
        if serialize:
            serialized: List[List[dict]] = []
            for stage in self.stages:
                group: List[dict] = []
                for position, operation in enumerate(stage.operations):
                    op_dict = operation.to_dict()
                    op_dict["_cte_stage"] = stage.stage
                    op_dict["_cte_position"] = position
                    if operation.context:
                        op_dict["_cte_request_context"] = operation.context.model_dump()
                    group.append(op_dict)
                serialized.append(group)
            return serialized

        return [stage.operations for stage in self.stages]

    def attach_context(self, ctx: ExecutionRequestContext) -> None:
        """Attach context to the entire plan hierarchy."""
        self.context = ctx
        for stage in self.stages:
            stage.attach_context(ctx)



class DependencyDAG(BaseModel):
    """Directed Acyclic Graph for dependency management.
    
    This class represents a directed acyclic graph of node dependencies,
    providing operations for dependency management, cycle detection, and
    topological sorting for execution planning. It can be used for any
    DAG-based problem including method dependencies, task scheduling,
    build systems, or workflow orchestration.
    
    Attributes:
        adjacency_list: Maps each node to its list of dependencies
    """
    adjacency_list: Dict[str, List[str]] = Field(default_factory=dict)
    
    def add_node(self, node: str) -> None:
        """Add a node to the DAG without dependencies.
        
        Args:
            node: Name of the node to add
        """
        if node not in self.adjacency_list:
            self.adjacency_list[node] = []
    
    def add_edge(self, from_node: str, to_node: str) -> None:
        """Add an edge (dependency) between two nodes.
        
        Args:
            from_node: The node that has a dependency
            to_node: The node that is depended upon
        """
        if from_node not in self.adjacency_list:
            self.adjacency_list[from_node] = []
        if to_node not in self.adjacency_list[from_node]:
            self.adjacency_list[from_node].append(to_node)
    
    def add_edges(self, from_node: str, to_nodes: List[str]) -> None:
        """Add multiple edges (dependencies) for a node.
        
        Args:
            from_node: The node that has dependencies
            to_nodes: List of nodes that are depended upon
        """
        if from_node not in self.adjacency_list:
            self.adjacency_list[from_node] = []
        for node in to_nodes:
            if node not in self.adjacency_list[from_node]:
                self.adjacency_list[from_node].append(node)
    
    def get_dependencies(self, node: str) -> List[str]:
        """Get all direct dependencies for a node.
        
        Args:
            node: The node to get dependencies for
            
        Returns:
            List of nodes that this node depends on
        """
        return self.adjacency_list.get(node, [])
    
    def get_all_dependencies(self, node: str) -> Set[str]:
        """Get all dependencies (direct and transitive) for a node.
        
        Args:
            node: The node to get all dependencies for
            
        Returns:
            Set of all nodes that this node depends on (directly or indirectly)
        """
        all_deps = set()
        to_process = deque(self.get_dependencies(node))
        
        while to_process:
            dep = to_process.popleft()
            if dep not in all_deps:
                all_deps.add(dep)
                to_process.extend(self.get_dependencies(dep))
        
        return all_deps
    
    def get_dependents(self, node: str) -> List[str]:
        """Get all nodes that directly depend on this node.
        
        Args:
            node: The node to find dependents for
            
        Returns:
            List of nodes that depend on this node
        """
        dependents = []
        for n, deps in self.adjacency_list.items():
            if node in deps:
                dependents.append(n)
        return dependents
    
    def get_all_dependents(self, node: str) -> Set[str]:
        """Get all dependents (direct and transitive) for a node.
        
        Args:
            node: The node to get all dependents for
            
        Returns:
            Set of all nodes that depend on this node (directly or indirectly)
        """
        all_dependents = set()
        to_process = deque(self.get_dependents(node))
        
        while to_process:
            dep = to_process.popleft()
            if dep not in all_dependents:
                all_dependents.add(dep)
                to_process.extend(self.get_dependents(dep))
        
        return all_dependents
    
    def get_all_nodes(self) -> Set[str]:
        """Get all nodes in the DAG.
        
        Returns:
            Set of all node names in the DAG
        """
        return set(self.adjacency_list.keys())
    
    def has_cycles(self) -> bool:
        """Check if the DAG has cycles using DFS.
        
        Returns:
            True if cycles are detected, False otherwise
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color = defaultdict(lambda: WHITE)
        
        def has_cycle_from(node: str) -> bool:
            """Check if there's a cycle starting from node."""
            if color[node] == GRAY:
                return True  # Back edge found (cycle)
            if color[node] == BLACK:
                return False  # Already processed
            
            color[node] = GRAY
            for neighbor in self.adjacency_list.get(node, []):
                if has_cycle_from(neighbor):
                    return True
            color[node] = BLACK
            return False
        
        # Check each component for cycles
        for node in self.adjacency_list:
            if color[node] == WHITE:
                if has_cycle_from(node):
                    return True
        
        return False
    
    def topological_sort(self) -> List[str]:
        """Return nodes in topological order using Kahn's algorithm.
        
        Returns:
            List of node names in topological order
            
        Raises:
            ValueError: If the graph contains cycles
        """
        if self.has_cycles():
            raise ValueError("Cannot perform topological sort on a graph with cycles")
        
        # Calculate in-degrees
        in_degree = defaultdict(int)
        for node in self.adjacency_list:
            in_degree[node] = 0
        
        for node, deps in self.adjacency_list.items():
            for dep in deps:
                in_degree[node] += 1
        
        # Find nodes with no dependencies
        queue = deque([node for node in self.adjacency_list if in_degree[node] == 0])
        result = []
        
        while queue:
            node = queue.popleft()
            result.append(node)
            
            # Remove this node from dependencies
            for dependent in self.get_dependents(node):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        return result
    
    def get_execution_stages(self) -> List[List[str]]:
        """Get execution stages where each stage contains nodes that can run in parallel.
        
        Returns:
            List of stages, where each stage is a list of nodes that can execute in parallel
            
        Raises:
            ValueError: If the graph contains cycles
        """
        if self.has_cycles():
            raise ValueError("Cannot create execution stages for a graph with cycles")
        
        stages = []
        in_degree = defaultdict(int)
        
        # Calculate initial in-degrees
        for node in self.adjacency_list:
            in_degree[node] = len(self.adjacency_list[node])
        
        processed = set()
        
        while len(processed) < len(self.adjacency_list):
            # Find all nodes with no remaining dependencies
            current_stage = [
                node for node in self.adjacency_list
                if in_degree[node] == 0 and node not in processed
            ]
            
            if not current_stage:
                # This shouldn't happen if there are no cycles
                raise ValueError("Could not create execution stages - possible hidden cycle")
            
            stages.append(current_stage)
            
            # Mark these as processed and update in-degrees
            for node in current_stage:
                processed.add(node)
                for dependent in self.get_dependents(node):
                    in_degree[dependent] -= 1
        
        return stages
    
    def is_reachable(self, from_node: str, to_node: str) -> bool:
        """Check if one node is reachable from another.
        
        Args:
            from_node: Starting node
            to_node: Target node
            
        Returns:
            True if to_node is reachable from from_node
        """
        return to_node in self.get_all_dependencies(from_node)
    
    def get_subgraph(self, nodes: Set[str]) -> 'DependencyDAG':
        """Get a subgraph containing only the specified nodes.
        
        Args:
            nodes: Set of nodes to include in the subgraph
            
        Returns:
            New DependencyDAG containing only the specified nodes
        """
        subgraph = DependencyDAG()
        for node in nodes:
            if node in self.adjacency_list:
                deps = [d for d in self.adjacency_list[node] if d in nodes]
                if deps:
                    subgraph.adjacency_list[node] = deps
                else:
                    subgraph.add_node(node)
        return subgraph
    
    def get_adjacency_list(self) -> Dict[str, List[str]]:
        """Get a copy of the adjacency list for external use.
        
        Returns:
            Copy of the adjacency list mapping
        """
        return dict(self.adjacency_list)
    
    def get_reverse_graph(self) -> Dict[str, List[str]]:
        """Get the reverse graph (dependents mapping).
        
        This creates a mapping from each node to all nodes that depend on it,
        essentially reversing the direction of edges in the graph.
        
        Returns:
            Dictionary mapping each node to its list of dependents
        """
        reverse = defaultdict(list)
        for node, deps in self.adjacency_list.items():
            for dep in deps:
                reverse[dep].append(node)
        return dict(reverse)
    
    def get_in_degrees(self) -> Dict[str, int]:
        """Calculate in-degrees for all nodes.
        
        The in-degree of a node is the number of edges pointing to it,
        which represents how many dependencies it has.
        
        Returns:
            Dictionary mapping each node to its in-degree count
        """
        in_degree = {node: 0 for node in self.adjacency_list}
        for node, deps in self.adjacency_list.items():
            for dep in deps:
                if dep not in in_degree:
                    in_degree[dep] = 0
            in_degree[node] = len(deps)
        return in_degree
    
    def remove_node(self, node: str) -> None:
        """Remove a node and all its edges from the DAG.
        
        Args:
            node: The node to remove from the graph
        """
        # Remove the node itself
        if node in self.adjacency_list:
            del self.adjacency_list[node]
        
        # Remove edges pointing to this node
        for deps in self.adjacency_list.values():
            if node in deps:
                deps.remove(node)
    
    def remove_edge(self, from_node: str, to_node: str) -> None:
        """Remove a specific edge from the DAG.
        
        Args:
            from_node: The source node of the edge
            to_node: The destination node of the edge
        """
        if from_node in self.adjacency_list:
            if to_node in self.adjacency_list[from_node]:
                self.adjacency_list[from_node].remove(to_node)


# Export all medallion types
__all__ = [
    'TableInfo',
    'LineageInfo',
    'ExecutionStage',
    'ExecutionPlan',
    'DependencyDAG',
]
