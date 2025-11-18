"""Tests for operation and execution plan serialization/deserialization."""

import pytest
from typing import Dict, Any

from core.types.base import CTEBaseModel
from core.operations import (
    BaseOperation,
    Insert,
    CreateTable,
    Update,
    Delete,
    ExecuteSQL
)
from core.operations.builder import OperationBuilder
from core.medallion.types.execution import ExecutionPlan, ExecutionStage
from core.medallion.types.lineage import LineageInfo
from core.types.metadata import SilverMetadata, QueryMetadata
from core.constants.sql import QueryType
from core.constants.compute import EngineType


class TestCTEBaseModel:
    """Test CTEBaseModel serialization functionality."""
    
    def test_simple_model_to_dict(self):
        """Test basic to_dict functionality."""
        class SimpleModel(CTEBaseModel):
            name: str
            value: int
            
        model = SimpleModel(name="test", value=42)
        result = model.to_dict()
        
        assert isinstance(result, dict)
        assert result["name"] == "test"
        assert result["value"] == 42
    
    def test_nested_model_to_dict(self):
        """Test nested CTEBaseModel serialization."""
        class InnerModel(CTEBaseModel):
            inner_value: str
            
        class OuterModel(CTEBaseModel):
            outer_value: int
            inner: InnerModel
            
        model = OuterModel(
            outer_value=10,
            inner=InnerModel(inner_value="nested")
        )
        result = model.to_dict()
        
        assert isinstance(result, dict)
        assert result["outer_value"] == 10
        assert isinstance(result["inner"], dict)
        assert result["inner"]["inner_value"] == "nested"
    
    def test_list_of_models_to_dict(self):
        """Test serialization of list containing CTEBaseModel instances."""
        class ItemModel(CTEBaseModel):
            id: int
            name: str
            
        class ContainerModel(CTEBaseModel):
            items: list[ItemModel]
            
        model = ContainerModel(
            items=[
                ItemModel(id=1, name="first"),
                ItemModel(id=2, name="second")
            ]
        )
        result = model.to_dict()
        
        assert isinstance(result["items"], list)
        assert len(result["items"]) == 2
        assert all(isinstance(item, dict) for item in result["items"])
        assert result["items"][0]["name"] == "first"
        assert result["items"][1]["name"] == "second"


class TestOperationSerialization:
    """Test operation serialization and deserialization."""
    
    def test_insert_operation_roundtrip(self):
        """Test Insert operation serialization and deserialization."""
        original = Insert(
            schema_name="silver",
            object_name="customers",
            source_query="SELECT * FROM bronze.raw_customers",
            mode="append"
        )
        
        # Serialize
        serialized = original.to_dict()
        assert isinstance(serialized, dict)
        assert serialized["operation_type"] == QueryType.INSERT.value
        assert serialized["schema_name"] == "silver"
        assert serialized["object_name"] == "customers"
        assert serialized["source_query"] == "SELECT * FROM bronze.raw_customers"
        assert serialized["mode"] == "append"
        
        # Deserialize
        restored = OperationBuilder.create_operation_from_dict(serialized)
        assert isinstance(restored, Insert)
        assert restored.schema_name == original.schema_name
        assert restored.object_name == original.object_name
        assert restored.source_query == original.source_query
        assert restored.mode == original.mode
    
    def test_create_table_operation_roundtrip(self):
        """Test CreateTable operation serialization and deserialization."""
        original = CreateTable(
            schema_name="silver",
            object_name="products",
            select_query="SELECT * FROM bronze.raw_products WHERE active = 1",
            recreate=True
        )
        
        # Serialize
        serialized = original.to_dict()
        assert serialized["operation_type"] == QueryType.CREATE_TABLE.value
        
        # Deserialize
        restored = OperationBuilder.create_operation_from_dict(serialized)
        assert isinstance(restored, CreateTable)
        assert restored.select_query == original.select_query
        assert restored.recreate == original.recreate
    
    def test_update_operation_with_metadata(self):
        """Test Update operation with QueryMetadata."""
        metadata = QueryMetadata(
            type=QueryType.UPDATE,
            table_name="customers",
            schema_name="silver",
            create_stats=True,
            stats_columns=["customer_id", "updated_at"]
        )
        
        original = Update(
            schema_name="silver",
            object_name="customers",
            set_columns={"status": "active", "updated_at": "GETDATE()"},
            where_clause="customer_id > 1000",
            metadata=metadata
        )
        
        # Serialize
        serialized = original.to_dict()
        assert "metadata" in serialized
        assert isinstance(serialized["metadata"], dict)
        assert serialized["metadata"]["create_stats"] is True
        
        # Deserialize
        restored = OperationBuilder.create_operation_from_dict(serialized)
        assert isinstance(restored, Update)
        assert restored.metadata is not None
        assert restored.metadata.create_stats is True
        assert restored.metadata.stats_columns == ["customer_id", "updated_at"]
    
    def test_execute_sql_operation_roundtrip(self):
        """Test ExecuteSQL operation serialization."""
        original = ExecuteSQL(
            schema_name="default",
            object_name="adhoc_query",
            sql="SELECT COUNT(*) FROM silver.customers",
            returns_results=True
        )
        
        # Serialize
        serialized = original.to_dict()
        assert serialized["operation_type"] == QueryType.EXECUTE_SQL.value
        assert serialized["sql"] == "SELECT COUNT(*) FROM silver.customers"
        assert serialized["returns_results"] is True
        
        # Deserialize
        restored = OperationBuilder.create_operation_from_dict(serialized)
        assert isinstance(restored, ExecuteSQL)
        assert restored.sql == original.sql
        assert restored.returns_results == original.returns_results
    
    def test_invalid_operation_type(self):
        """Test handling of invalid operation type."""
        invalid_dict = {
            "operation_type": "INVALID_TYPE",
            "schema_name": "test",
            "object_name": "test"
        }
        
        with pytest.raises(ValueError) as exc_info:
            OperationBuilder.create_operation_from_dict(invalid_dict)
        assert "Invalid operation_type" in str(exc_info.value)
    
    def test_missing_operation_type(self):
        """Test handling of missing operation type."""
        invalid_dict = {
            "schema_name": "test",
            "object_name": "test"
        }
        
        with pytest.raises(ValueError) as exc_info:
            OperationBuilder.create_operation_from_dict(invalid_dict)
        assert "operation_type is required" in str(exc_info.value)


class TestExecutionPlanSerialization:
    """Test ExecutionPlan serialization."""
    
    def test_execution_plan_to_dict(self):
        """Test ExecutionPlan.to_dict() method."""
        # Create operations
        op1 = CreateTable(schema_name="silver", object_name="table1", select_query="SELECT * FROM bronze.table1")
        op2 = Insert(schema_name="silver", object_name="table2", source_query="SELECT * FROM silver.table1")
        
        # Create stages
        stage1 = ExecutionStage(stage=1, operations=[op1])
        stage2 = ExecutionStage(stage=2, operations=[op2])
        
        # Create metadata and lineage
        metadata = SilverMetadata(
            sp_name="Load_Test",
            group_file_name="test_group.json",
            preferred_engine=EngineType.SQL
        )
        lineage = LineageInfo(lineage_data={"source": "bronze", "target": "silver"})
        
        # Create execution plan
        plan = ExecutionPlan(
            sequencer_name="TestSequencer",
            metadata=metadata,
            lineage=lineage,
            total_queries=2,
            stages=[stage1, stage2],
            dependency_graph={"table2": ["table1"]}
        )
        
        # Serialize entire plan
        plan_dict = plan.to_dict()
        
        assert isinstance(plan_dict, dict)
        assert plan_dict["sequencer_name"] == "TestSequencer"
        assert isinstance(plan_dict["metadata"], dict)
        assert plan_dict["metadata"]["sp_name"] == "Load_Test"
        assert isinstance(plan_dict["lineage"], dict)
        assert plan_dict["total_queries"] == 2
        assert len(plan_dict["stages"]) == 2
        assert isinstance(plan_dict["stages"][0], dict)
        assert len(plan_dict["stages"][0]["operations"]) == 1
        assert isinstance(plan_dict["stages"][0]["operations"][0], dict)
    
    def test_execution_plan_get_all_operations_serialize(self):
        """Test ExecutionPlan.get_all_operations(serialize=True)."""
        # Create operations
        op1 = CreateTable(schema_name="silver", object_name="table1", select_query="SELECT 1")
        op2 = Insert(schema_name="silver", object_name="table2", source_query="SELECT 2")
        op3 = Update(schema_name="silver", object_name="table3", set_columns={"col": "val"})
        
        # Create stages
        stage1 = ExecutionStage(stage=1, operations=[op1, op2])  # Parallel ops
        stage2 = ExecutionStage(stage=2, operations=[op3])
        
        # Create execution plan
        plan = ExecutionPlan(
            sequencer_name="TestSequencer",
            metadata=SilverMetadata(sp_name="test", group_file_name="test.json"),
            lineage=LineageInfo(),
            total_queries=3,
            stages=[stage1, stage2],
            dependency_graph={}
        )
        
        # Get serialized operations grouped by stage
        serialized_stages = plan.get_all_operations(serialize=True)
        
        assert isinstance(serialized_stages, list)
        assert len(serialized_stages) == 2  # Two stages
        
        # First stage has two operations
        assert len(serialized_stages[0]) == 2
        assert all(isinstance(op, dict) for op in serialized_stages[0])
        assert serialized_stages[0][0]["operation_type"] == QueryType.CREATE_TABLE.value
        assert serialized_stages[0][1]["operation_type"] == QueryType.INSERT.value
        
        # Second stage has one operation
        assert len(serialized_stages[1]) == 1
        assert serialized_stages[1][0]["operation_type"] == QueryType.UPDATE.value
    
    def test_execution_plan_get_all_operations_staged(self):
        """Test ExecutionPlan.get_all_operations() returns stage-grouped operations."""
        op1 = CreateTable(schema_name="silver", object_name="table1", select_query="SELECT 1")
        op2 = Insert(schema_name="silver", object_name="table2", source_query="SELECT 2")
        op3 = Update(schema_name="silver", object_name="table3", set_columns={"col": "val"})
        
        # Create stages with different numbers of operations
        stage1 = ExecutionStage(stage=1, operations=[op1, op2])  # Parallel ops
        stage2 = ExecutionStage(stage=2, operations=[op3])
        
        plan = ExecutionPlan(
            sequencer_name="TestSequencer",
            metadata=SilverMetadata(sp_name="test", group_file_name="test.json"),
            lineage=LineageInfo(),
            total_queries=3,
            stages=[stage1, stage2],
            dependency_graph={"table3": ["table1", "table2"]}
        )
        
        # Get stage-grouped operations
        operations = plan.get_all_operations(serialize=False)
        
        assert isinstance(operations, list)
        assert len(operations) == 2  # Two stages
        
        # First stage has two operations that can run in parallel
        assert isinstance(operations[0], list)
        assert len(operations[0]) == 2
        assert all(isinstance(op, BaseOperation) for op in operations[0])
        assert operations[0][0] == op1
        assert operations[0][1] == op2
        
        # Second stage has one operation
        assert isinstance(operations[1], list)
        assert len(operations[1]) == 1
        assert isinstance(operations[1][0], BaseOperation)
        assert operations[1][0] == op3
    
    def test_execution_plan_complete_serialization(self):
        """Test complete ExecutionPlan serialization with all nested structures."""
        # Create complex operations with metadata
        query_metadata1 = QueryMetadata(
            type=QueryType.CREATE_TABLE,
            table_name="products",
            schema_name="silver",
            create_stats=True,
            stats_columns=["product_id", "category_id"],
            preferred_engine=EngineType.SQL
        )
        
        query_metadata2 = QueryMetadata(
            type=QueryType.INSERT,
            table_name="sales",
            schema_name="silver",
            create_stats=False
        )
        
        # Create various operations with different configurations
        op1 = CreateTable(
            schema_name="silver",
            object_name="products",
            select_query="SELECT * FROM bronze.raw_products WHERE active = 1",
            recreate=True,
            metadata=query_metadata1,
            logging_context={"batch_id": "2024-01-01", "source": "etl_pipeline"}
        )
        
        op2 = Insert(
            schema_name="silver",
            object_name="sales",
            source_query="SELECT * FROM bronze.raw_sales",
            mode="append",
            metadata=query_metadata2,
            engine_hint=EngineType.SPARK
        )
        
        op3 = Update(
            schema_name="silver",
            object_name="customers",
            set_columns={"last_updated": "GETDATE()", "status": "active"},
            where_clause="customer_id > 1000"
        )
        
        op4 = Delete(
            schema_name="silver",
            object_name="expired_records",
            where_clause="expiry_date < GETDATE()"
        )
        
        from core.operations import CreateStatistics
        op5 = CreateStatistics(
            schema_name="silver",
            object_name="products",
            columns=["product_id", "category_id"],
            with_fullscan=True,
            auto_discover=False
        )
        
        # Create stages with multiple operations
        stage1 = ExecutionStage(stage=1, operations=[op1, op2])  # Parallel operations
        stage2 = ExecutionStage(stage=2, operations=[op3])
        stage3 = ExecutionStage(stage=3, operations=[op4, op5])
        
        # Create comprehensive metadata
        silver_metadata = SilverMetadata(
            sp_name="Load_Complex_Pipeline",
            group_file_name="complex_group.json",
            description="Complex ETL pipeline for testing",
            tags=["test", "complex", "multi-stage"],
            preferred_engine=EngineType.SQL
        )
        
        # Create detailed lineage
        lineage = LineageInfo(
            lineage_data={
                "sources": ["bronze.raw_products", "bronze.raw_sales"],
                "targets": ["silver.products", "silver.sales", "silver.customers"],
                "transformations": ["filter", "aggregate", "join"],
                "timestamp": "2024-01-01T10:00:00Z"
            }
        )
        
        # Create execution plan with complex dependency graph
        plan = ExecutionPlan(
            sequencer_name="ComplexTestSequencer",
            metadata=silver_metadata,
            lineage=lineage,
            total_queries=5,
            stages=[stage1, stage2, stage3],
            dependency_graph={
                "sales": ["products"],
                "customers": ["sales", "products"],
                "expired_records": ["customers"]
            }
        )
        
        # Serialize the entire plan
        plan_dict = plan.to_dict()
        
        # Verify top-level structure
        assert isinstance(plan_dict, dict)
        assert plan_dict["sequencer_name"] == "ComplexTestSequencer"
        assert plan_dict["total_queries"] == 5
        
        # Verify metadata is fully serialized
        assert isinstance(plan_dict["metadata"], dict)
        assert plan_dict["metadata"]["sp_name"] == "Load_Complex_Pipeline"
        assert plan_dict["metadata"]["description"] == "Complex ETL pipeline for testing"
        assert plan_dict["metadata"]["tags"] == ["test", "complex", "multi-stage"]
        assert plan_dict["metadata"]["preferred_engine"] == "sql"  # Enum as string (lowercase)
        
        # Verify lineage is fully serialized
        assert isinstance(plan_dict["lineage"], dict)
        assert isinstance(plan_dict["lineage"]["lineage_data"], dict)
        assert "sources" in plan_dict["lineage"]["lineage_data"]
        assert "timestamp" in plan_dict["lineage"]["lineage_data"]
        
        # Verify dependency graph
        assert isinstance(plan_dict["dependency_graph"], dict)
        assert plan_dict["dependency_graph"]["sales"] == ["products"]
        assert len(plan_dict["dependency_graph"]) == 3
        
        # Verify stages structure
        assert isinstance(plan_dict["stages"], list)
        assert len(plan_dict["stages"]) == 3
        
        # Verify first stage with parallel operations
        stage1_dict = plan_dict["stages"][0]
        assert isinstance(stage1_dict, dict)
        assert stage1_dict["stage"] == 1
        assert isinstance(stage1_dict["operations"], list)
        assert len(stage1_dict["operations"]) == 2
        
        # Verify operation with metadata and logging_context
        op1_dict = stage1_dict["operations"][0]
        assert isinstance(op1_dict, dict)
        assert op1_dict["operation_type"] == QueryType.CREATE_TABLE.value
        assert op1_dict["schema_name"] == "silver"
        assert op1_dict["object_name"] == "products"
        assert op1_dict["recreate"] is True
        
        # Verify nested metadata is serialized
        assert isinstance(op1_dict["metadata"], dict)
        assert op1_dict["metadata"]["type"] == QueryType.CREATE_TABLE.value
        assert op1_dict["metadata"]["create_stats"] is True
        assert op1_dict["metadata"]["stats_columns"] == ["product_id", "category_id"]
        
        # Verify logging_context is serialized
        assert isinstance(op1_dict["logging_context"], dict)
        assert op1_dict["logging_context"]["batch_id"] == "2024-01-01"
        
        # Verify operation with engine_hint
        op2_dict = stage1_dict["operations"][1]
        assert op2_dict["engine_hint"] == EngineType.SPARK.value
        
        # Verify all nested structures are dicts, not Pydantic models
        def assert_no_pydantic_models(obj, path=""):
            """Recursively check that no Pydantic models remain."""
            if isinstance(obj, dict):
                for key, value in obj.items():
                    assert_no_pydantic_models(value, f"{path}.{key}")
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    assert_no_pydantic_models(item, f"{path}[{i}]")
            else:
                # Should not be a CTEBaseModel instance
                assert not isinstance(obj, CTEBaseModel), f"Found CTEBaseModel at {path}: {obj}"
        
        assert_no_pydantic_models(plan_dict)
        
        # Verify the dict can be JSON serialized
        import json
        json_str = json.dumps(plan_dict)
        assert isinstance(json_str, str)
        
        # Verify round-trip: deserialize operations from dict
        from core.operations.builder import OperationBuilder
        
        # Recreate first operation from dict
        recreated_op1 = OperationBuilder.create_operation_from_dict(op1_dict)
        assert isinstance(recreated_op1, CreateTable)
        assert recreated_op1.schema_name == op1.schema_name
        assert recreated_op1.object_name == op1.object_name
        assert recreated_op1.metadata.create_stats == op1.metadata.create_stats
    
    def test_execution_plan_edge_cases(self):
        """Test ExecutionPlan serialization with edge cases."""
        # Create minimal operations without metadata
        op1 = ExecuteSQL(
            schema_name="default",
            object_name="test",
            sql="SELECT 1"
        )
        
        # Empty stages list plan
        empty_plan = ExecutionPlan(
            sequencer_name="EmptyPlan",
            metadata=SilverMetadata(sp_name="Empty", group_file_name="empty.json"),
            lineage=LineageInfo(),
            total_queries=0,
            stages=[],
            dependency_graph={}
        )
        
        empty_dict = empty_plan.to_dict()
        assert empty_dict["stages"] == []
        assert empty_dict["dependency_graph"] == {}
        assert empty_dict["total_queries"] == 0
        
        # Plan with operations that have None values
        op_with_nulls = Insert(
            schema_name="test",
            object_name="test_table",
            source_query="SELECT * FROM source",
            metadata=None,  # No metadata
            engine_hint=None,  # No engine hint
            logging_context={}  # Empty logging context
        )
        
        stage = ExecutionStage(stage=1, operations=[op_with_nulls])
        
        plan_with_nulls = ExecutionPlan(
            sequencer_name="NullPlan",
            metadata=SilverMetadata(
                sp_name="Test",
                group_file_name="test.json",
                description=None,  # Optional field as None
                tags=[]  # Empty list
            ),
            lineage=LineageInfo(lineage_data={}),  # Empty lineage data
            total_queries=1,
            stages=[stage],
            dependency_graph={}
        )
        
        null_dict = plan_with_nulls.to_dict()
        
        # Verify None values are excluded
        op_dict = null_dict["stages"][0]["operations"][0]
        assert "metadata" not in op_dict  # None should be excluded
        assert "engine_hint" not in op_dict  # None should be excluded
        assert op_dict["logging_context"] == {}  # Empty dict is preserved
        
        # Metadata with None description should be excluded
        assert "description" not in null_dict["metadata"]
        assert null_dict["metadata"]["tags"] == []  # Empty list preserved


class TestMetadataSerialization:
    """Test metadata classes serialization."""
    
    def test_silver_metadata_to_dict(self):
        """Test SilverMetadata serialization."""
        metadata = SilverMetadata(
            sp_name="Load_Customer",
            group_file_name="customer_group.json",
            description="Customer data processing",
            tags=["customer", "daily"],
            preferred_engine=EngineType.SPARK
        )
        
        result = metadata.to_dict()
        
        assert isinstance(result, dict)
        assert result["sp_name"] == "Load_Customer"
        assert result["group_file_name"] == "customer_group.json"
        assert result["description"] == "Customer data processing"
        assert result["tags"] == ["customer", "daily"]
        assert result["preferred_engine"] == EngineType.SPARK.value  # Enum serialized to string value
    
    def test_query_metadata_to_dict(self):
        """Test QueryMetadata serialization."""
        metadata = QueryMetadata(
            type=QueryType.INSERT,
            table_name="customers",
            schema_name="silver",
            create_stats=True,
            stats_columns=["id", "name"],
            preferred_engine=EngineType.SQL
        )
        
        result = metadata.to_dict()
        
        assert isinstance(result, dict)
        assert result["type"] == "INSERT"
        assert result["table_name"] == "customers"
        assert result["create_stats"] is True
        assert result["stats_columns"] == ["id", "name"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])