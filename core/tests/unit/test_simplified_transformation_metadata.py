"""Test the simplified TransformationMetadata structure."""

import pytest
from core.medallion.silver.decorators import silver_metadata
from core.medallion.silver.metadata_discovery import TransformationMetadata
from core.medallion.silver.sequencer import SilverTransformationSequencer
from core.types import SilverMetadata
from core.constants.compute import EngineType


class TestSimplifiedTransformationMetadata:
    """Test the simplified TransformationMetadata with properties."""
    
    def test_essential_fields_only(self):
        """Test that TransformationMetadata only stores essential fields."""
        
        @silver_metadata(
            sp_name="Load_Test_Dim",
            group_file_name="test/group.json",
            description="Test description",
            tags=["test", "example"]
        )
        class TestSequencer(SilverTransformationSequencer):
            pass
        
        original = TestSequencer._silver_metadata
        
        # Create metadata with only essential fields
        metadata = TransformationMetadata(
            sp_name=original.sp_name,
            model_name="test_model",
            sequencer_class=TestSequencer,
            silver_metadata=original
        )
        
        # Essential fields are stored directly
        assert metadata.sp_name == "Load_Test_Dim"
        assert metadata.model_name == "test_model"
        assert metadata.sequencer_class is TestSequencer
        assert metadata.silver_metadata is original
    
    def test_properties_access_silver_metadata(self):
        """Test that properties access fields from silver_metadata."""
        
        original = SilverMetadata(
            sp_name="Load_Customer_Dim",
            group_file_name="dimensions/customer.json",
            description="Customer dimension",
            tags=["dimension", "customer"],
            preferred_engine=EngineType.SPARK
        )
        
        metadata = TransformationMetadata(
            sp_name=original.sp_name,
            model_name="sales",
            sequencer_class=type('TestSeq', (SilverTransformationSequencer,), {}),
            silver_metadata=original
        )
        
        # Properties access silver_metadata
        assert metadata.description == "Customer dimension"
        assert metadata.tags == ["dimension", "customer"]
        assert metadata.group_file_name == "dimensions/customer.json"
        assert metadata.preferred_engine == "spark"
    
    def test_computed_properties(self):
        """Test computed properties like silver_table_name and module_path."""
        
        @silver_metadata(
            sp_name="Load_Product_Fact",
            group_file_name="facts/product.json"
        )
        class ProductFactETL(SilverTransformationSequencer):
            pass
        
        metadata = TransformationMetadata(
            sp_name="Load_Product_Fact",
            model_name="sales",
            sequencer_class=ProductFactETL,
            silver_metadata=ProductFactETL._silver_metadata
        )
        
        # Computed properties
        assert metadata.silver_table_name == "Product_Fact"
        assert ProductFactETL.__name__ in metadata.module_path
    
    def test_to_dict_includes_properties(self):
        """Test that to_dict includes values from properties."""
        
        original = SilverMetadata(
            sp_name="Load_Test_SP",
            group_file_name="test/file.json",
            description="Test",
            tags=["tag1", "tag2"],
            preferred_engine=EngineType.SQL
        )
        
        metadata = TransformationMetadata(
            sp_name=original.sp_name,
            model_name="test",
            sequencer_class=type('TestClass', (SilverTransformationSequencer,), {}),
            silver_metadata=original
        )
        
        dict_result = metadata.to_dict()
        
        # Dictionary includes property values
        assert dict_result['sp_name'] == "Load_Test_SP"
        assert dict_result['model_name'] == "test"
        assert dict_result['silver_table_name'] == "Test_SP"
        assert dict_result['description'] == "Test"
        assert dict_result['tags'] == ["tag1", "tag2"]
        assert dict_result['preferred_engine'] == "sql"
        assert 'sequencer_class_name' in dict_result
    
    def test_no_redundant_storage(self):
        """Test that we're not storing redundant data."""
        
        original = SilverMetadata(
            sp_name="Load_No_Redundancy",
            group_file_name="test/redundancy.json",
            description="No redundancy test"
        )
        
        metadata = TransformationMetadata(
            sp_name=original.sp_name,
            model_name="test",
            sequencer_class=type('NoRedundancy', (SilverTransformationSequencer,), {}),
            silver_metadata=original
        )
        
        # Check the actual dataclass fields
        import dataclasses
        field_names = [f.name for f in dataclasses.fields(metadata)]
        
        # Should only have the 4 essential fields
        assert len(field_names) == 4
        assert 'sp_name' in field_names
        assert 'model_name' in field_names
        assert 'sequencer_class' in field_names
        assert 'silver_metadata' in field_names
        
        # These should be properties, not stored fields
        # They access data from silver_metadata
        assert metadata.description == original.description
        assert metadata.group_file_name == original.group_file_name


if __name__ == "__main__":
    pytest.main([__file__, "-v"])