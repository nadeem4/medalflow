"""Test that TransformationMetadata stores the original SilverMetadata."""

import pytest
from core.medallion.silver.decorators import silver_metadata
from core.medallion.silver.metadata_discovery import TransformationMetadata
from core.types import SilverMetadata


class TestSilverMetadataStorage:
    """Test storing original SilverMetadata in TransformationMetadata."""
    
    def test_transformation_metadata_stores_silver_metadata(self):
        """Test that we store the complete SilverMetadata object."""
        
        # Create SilverMetadata
        original_metadata = SilverMetadata(
            sp_name="Test_SP",
            group_file_name="test/group.json",
            description="Test description",
            tags=["test", "example"],
            disabled=True
        )
        
        # Create TransformationMetadata with original metadata
        trans_metadata = TransformationMetadata(
            sp_name=original_metadata.sp_name,
            model_name="test_model",
            silver_table_name="test_table",
            silver_metadata=original_metadata
        )
        
        # Verify original metadata is stored
        assert trans_metadata.silver_metadata is not None
        assert trans_metadata.silver_metadata is original_metadata
        assert trans_metadata.silver_metadata.sp_name == "Test_SP"
        assert trans_metadata.silver_metadata.description == "Test description"
        assert trans_metadata.silver_metadata.disabled is True
        assert "test" in trans_metadata.silver_metadata.tags
    
    def test_access_through_silver_metadata(self):
        """Test accessing fields through the stored silver_metadata."""
        
        original = SilverMetadata(
            sp_name="Load_Customer_Dim",
            group_file_name="dimensions/customer.json",
            description="Customer dimension",
            disabled=False
        )
        
        trans_metadata = TransformationMetadata(
            sp_name=original.sp_name,
            model_name="sales",
            silver_table_name="dim_customer",
            silver_metadata=original
        )
        
        # Can access through silver_metadata
        assert trans_metadata.silver_metadata.sp_name == "Load_Customer_Dim"
        assert trans_metadata.silver_metadata.group_file_name == "dimensions/customer.json"
        assert trans_metadata.silver_metadata.description == "Customer dimension"
        assert trans_metadata.silver_metadata.disabled is False
        
        # Also have direct access to common fields
        assert trans_metadata.sp_name == "Load_Customer_Dim"
        assert trans_metadata.is_disabled is False
    
    def test_decorator_metadata_preserved(self):
        """Test that decorator metadata is preserved through discovery."""
        
        @silver_metadata(
            sp_name="Decorated_SP",
            group_file_name="test/decorated.json",
            description="Decorated transformation",
            tags=["decorated", "test"],
            disabled=True
        )
        class DecoratedSequencer:
            pass
        
        # The decorator attaches _silver_metadata
        assert hasattr(DecoratedSequencer, '_silver_metadata')
        original = DecoratedSequencer._silver_metadata
        
        # Simulate what discovery does
        trans_metadata = TransformationMetadata(
            sp_name=original.sp_name,
            model_name="test",
            silver_table_name="test_table",
            silver_metadata=original,
            sequencer_class=DecoratedSequencer
        )
        
        # Original metadata is preserved
        assert trans_metadata.silver_metadata is original
        assert trans_metadata.silver_metadata.sp_name == "Decorated_SP"
        assert trans_metadata.silver_metadata.description == "Decorated transformation"
        assert trans_metadata.silver_metadata.disabled is True
        assert "decorated" in trans_metadata.silver_metadata.tags
    
    def test_benefits_of_storing_original(self):
        """Demonstrate benefits of storing the original metadata."""
        
        original = SilverMetadata(
            sp_name="Complete_SP",
            group_file_name="complete/test.json",
            description="Complete metadata example",
            tags=["complete", "all-fields"],
            disabled=False,
            disable_key_reshuffling=True,
            # Any future fields added to SilverMetadata will be preserved
        )
        
        trans_metadata = TransformationMetadata(
            sp_name=original.sp_name,
            model_name="complete",
            silver_table_name="complete_table",
            silver_metadata=original
        )
        
        # Benefits:
        # 1. Access to all fields without duplication
        assert trans_metadata.silver_metadata.disable_key_reshuffling is True
        
        # 2. Future-proof - new fields automatically available
        # If SilverMetadata gets new fields, they're accessible through silver_metadata
        
        # 3. Can still have computed/convenience fields
        assert trans_metadata.silver_table_name == "complete_table"  # Computed field
        assert trans_metadata.sp_name == "Complete_SP"  # Convenience access
        
        # 4. Single source of truth
        assert trans_metadata.silver_metadata.sp_name == trans_metadata.sp_name


if __name__ == "__main__":
    pytest.main([__file__, "-v"])