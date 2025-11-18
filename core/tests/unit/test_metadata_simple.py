"""Simple unit tests for TransformationMetadata without full system setup."""

import pytest
from core.medallion.silver.decorators import silver_metadata
from core.medallion.silver.metadata_discovery import TransformationMetadata


class TestTransformationMetadata:
    """Test TransformationMetadata dataclass directly."""
    
    def test_transformation_metadata_creation(self):
        """Test creating TransformationMetadata with sequencer_class."""
        
        @silver_metadata(
            sp_name="Test_SP",
            group_file_name="test/group.json"
        )
        class TestSequencer:
            pass
        
        metadata = TransformationMetadata(
            sp_name="Test_SP",
            model_name="test",
            silver_table_name="test_table",
            sequencer_class=TestSequencer
        )
        
        assert metadata.sp_name == "Test_SP"
        assert metadata.sequencer_class is TestSequencer
        assert metadata.is_disabled is False  # Default value
    
    def test_to_dict_with_sequencer_class(self):
        """Test to_dict includes sequencer class name."""
        
        class MySequencer:
            pass
        
        metadata = TransformationMetadata(
            sp_name="Test_SP",
            model_name="test",
            silver_table_name="test_table",
            sequencer_class=MySequencer
        )
        
        result = metadata.to_dict()
        assert result['sequencer_class_name'] == 'MySequencer'
        assert 'decorator_type' not in result
    
    def test_to_dict_without_sequencer_class(self):
        """Test to_dict handles None sequencer_class."""
        
        metadata = TransformationMetadata(
            sp_name="Test_SP",
            model_name="test",
            silver_table_name="test_table",
            sequencer_class=None
        )
        
        result = metadata.to_dict()
        assert result['sequencer_class_name'] is None
    
    def test_disabled_metadata_with_sequencer(self):
        """Test disabled metadata still stores sequencer class."""
        
        class DisabledSequencer:
            pass
        
        metadata = TransformationMetadata(
            sp_name="Disabled_SP",
            model_name="test",
            silver_table_name="test_table",
            is_disabled=True,
            sequencer_class=DisabledSequencer
        )
        
        assert metadata.is_disabled is True
        assert metadata.sequencer_class is DisabledSequencer
    
    def test_silver_metadata_decorator_attaches_metadata(self):
        """Test that silver_metadata decorator attaches _silver_metadata."""
        
        @silver_metadata(
            sp_name="Decorated_SP",
            group_file_name="test/group.json",
            disabled=True
        )
        class DecoratedSequencer:
            pass
        
        assert hasattr(DecoratedSequencer, '_silver_metadata')
        assert DecoratedSequencer._silver_metadata.sp_name == "Decorated_SP"
        assert DecoratedSequencer._silver_metadata.disabled is True
    
    def test_no_etl_metadata_alias(self):
        """Test that etl_metadata alias no longer exists."""
        from core.medallion.silver import decorators
        
        assert hasattr(decorators, 'silver_metadata')
        assert not hasattr(decorators, 'etl_metadata')


if __name__ == "__main__":
    pytest.main([__file__, "-v"])