"""Tests for the simplified TransformationMetadata implementation."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from core.medallion.silver.decorators import silver_metadata
from core.medallion.silver.metadata_discovery import TransformationMetadata


class TestSimplifiedMetadata:
    """Test suite for simplified metadata discovery."""
    
    def test_transformation_metadata_has_sequencer_class(self):
        """Test that TransformationMetadata stores class reference."""
        @silver_metadata(
            sp_name="Test_SP",
            group_file_name="test/group.json"
        )
        class TestSequencer:
            pass
        
        discovery = SilverMetadataDiscovery()
        metadata = discovery._extract_metadata_from_class(TestSequencer)
        
        assert metadata is not None
        assert metadata.sequencer_class is TestSequencer
        assert metadata.module_path == f"{TestSequencer.__module__}.{TestSequencer.__name__}"
    
    def test_to_dict_includes_sequencer_class_name(self):
        """Test that to_dict includes sequencer class name."""
        @silver_metadata(
            sp_name="Test_SP",
            group_file_name="test/group.json"
        )
        class TestSequencer:
            pass
        
        discovery = SilverMetadataDiscovery()
        metadata = discovery._extract_metadata_from_class(TestSequencer)
        
        dict_result = metadata.to_dict()
        assert 'sequencer_class_name' in dict_result
        assert dict_result['sequencer_class_name'] == 'TestSequencer'
        assert 'decorator_type' not in dict_result  # Should be removed
    
    def test_only_silver_metadata_decorator_supported(self):
        """Test that only silver_metadata decorator is recognized."""
        # Class without decorator
        class NoDecorator:
            pass
        
        # Class with wrong decorator
        class WrongDecorator:
            _etl_metadata = Mock()  # Simulate old decorator
        
        # Class with correct decorator
        @silver_metadata(
            sp_name="Test_SP",
            group_file_name="test/group.json"
        )
        class CorrectDecorator:
            pass
        
        discovery = SilverMetadataDiscovery()
        
        assert not discovery._is_transformation_class(NoDecorator)
        assert not discovery._is_transformation_class(WrongDecorator)
        assert discovery._is_transformation_class(CorrectDecorator)
    
    def test_extract_metadata_returns_none_for_unsupported_decorator(self):
        """Test that extract_metadata returns None for classes without silver_metadata."""
        class NoDecorator:
            pass
        
        class OldDecorator:
            _etl_metadata = Mock()
        
        discovery = SilverMetadataDiscovery()
        
        assert discovery._extract_metadata_from_class(NoDecorator) is None
        assert discovery._extract_metadata_from_class(OldDecorator) is None
    
    def test_can_instantiate_sequencer_from_metadata(self):
        """Test that we can instantiate a sequencer from the stored class reference."""
        @silver_metadata(
            sp_name="Test_SP",
            group_file_name="test/group.json"
        )
        class TestSequencer:
            def __init__(self):
                self.initialized = True
        
        discovery = SilverMetadataDiscovery()
        metadata = discovery._extract_metadata_from_class(TestSequencer)
        
        # Should be able to instantiate the sequencer
        instance = metadata.sequencer_class()
        assert instance.initialized is True
        assert isinstance(instance, TestSequencer)
    
    def test_disabled_transformations_with_class_reference(self):
        """Test that disabled transformations still store class reference."""
        @silver_metadata(
            sp_name="Disabled_SP",
            group_file_name="test/group.json",
            disabled=True
        )
        class DisabledSequencer:
            pass
        
        discovery = SilverMetadataDiscovery()
        metadata = discovery._extract_metadata_from_class(DisabledSequencer)
        
        assert metadata is not None
        assert metadata.is_disabled is True
        assert metadata.sequencer_class is DisabledSequencer
    
    def test_discovery_flow_with_simplified_metadata(self):
        """Test the complete discovery flow with simplified metadata."""
        @silver_metadata(
            sp_name="Complete_Test_SP",
            group_file_name="complete/test.json",
            description="Test transformation"
        )
        class CompleteTestSequencer:
            pass
        
        discovery = SilverMetadataDiscovery()
        
        # Add to cache manually for testing
        metadata = discovery._extract_metadata_from_class(CompleteTestSequencer)
        if metadata:
            discovery._metadata_cache[metadata.sp_name] = metadata
            discovery._discovery_complete = True
        
        # Retrieve and verify
        retrieved = discovery.get_transformation_by_sp("Complete_Test_SP")
        assert retrieved is not None
        assert retrieved.sequencer_class is CompleteTestSequencer
        assert retrieved.description == "Test transformation"
        
        # Verify it can be converted to dict for serialization
        dict_result = retrieved.to_dict()
        assert dict_result['sp_name'] == "Complete_Test_SP"
        assert dict_result['sequencer_class_name'] == "CompleteTestSequencer"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])