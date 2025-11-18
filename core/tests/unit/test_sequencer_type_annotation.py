"""Test that sequencer_class has proper type annotation."""

from typing import Type, Optional
from core.medallion.silver.metadata_discovery import TransformationMetadata
from core.medallion.silver.sequencer import SilverTransformationSequencer
from core.medallion.silver.decorators import silver_metadata


class TestSequencerTypeAnnotation:
    """Test proper type annotation for sequencer_class."""
    
    def test_transformation_metadata_accepts_silver_sequencer(self):
        """Test that TransformationMetadata accepts SilverTransformationSequencer subclasses."""
        
        @silver_metadata(
            sp_name="Test_SP",
            group_file_name="test/group.json"
        )
        class TestSilverSequencer(SilverTransformationSequencer):
            pass
        
        # Should accept a subclass of SilverTransformationSequencer
        metadata = TransformationMetadata(
            sp_name="Test_SP",
            model_name="test",
            silver_table_name="test_table",
            sequencer_class=TestSilverSequencer  # Type: Type[SilverTransformationSequencer]
        )
        
        assert metadata.sequencer_class is TestSilverSequencer
        assert issubclass(metadata.sequencer_class, SilverTransformationSequencer)
    
    def test_type_annotation_is_correct(self):
        """Verify the type annotation is Type[SilverTransformationSequencer]."""
        import inspect
        from typing import get_type_hints
        
        # Get type hints for TransformationMetadata
        hints = get_type_hints(TransformationMetadata)
        
        # Check sequencer_class type
        sequencer_class_type = hints.get('sequencer_class')
        
        # Should be Optional[Type[SilverTransformationSequencer]]
        assert sequencer_class_type is not None
        
        # The type should include SilverTransformationSequencer
        # Note: The exact check depends on Python version and typing implementation
        type_str = str(sequencer_class_type)
        assert 'SilverTransformationSequencer' in type_str
    
    def test_can_instantiate_from_sequencer_class(self):
        """Test that we can instantiate a sequencer from the class reference."""
        
        class MockSettings:
            def __init__(self):
                self.silver_package_name = "test"
        
        @silver_metadata(
            sp_name="Test_SP",
            group_file_name="test/group.json"
        )
        class TestSequencer(SilverTransformationSequencer):
            def __init__(self, sql_dialect: str = "tsql"):
                # Mock initialization to avoid full system setup
                self.initialized = True
                self.layer = "SILVER"
        
        metadata = TransformationMetadata(
            sp_name="Test_SP",
            model_name="test",
            silver_table_name="test_table",
            sequencer_class=TestSequencer
        )
        
        # Should be able to instantiate
        instance = metadata.sequencer_class()
        assert instance.initialized is True
        assert isinstance(instance, SilverTransformationSequencer)


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])