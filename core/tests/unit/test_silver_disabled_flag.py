"""Unit tests for the Silver metadata disabled flag functionality."""

import pytest
from unittest.mock import Mock, patch
from core.types.metadata import SilverMetadata
from core.medallion.silver.decorators import silver_metadata
from core.medallion.silver.metadata_discovery import SilverMetadataDiscovery, TransformationMetadata


class TestSilverDisabledFlag:
    """Test suite for the disabled flag in Silver transformations."""
    
    def test_silver_metadata_disabled_default_false(self):
        """Test that SilverMetadata disabled field defaults to False."""
        metadata = SilverMetadata(
            sp_name="Test_SP",
            group_file_name="test/group.json"
        )
        assert metadata.disabled is False
    
    def test_silver_metadata_disabled_can_be_set_true(self):
        """Test that SilverMetadata disabled field can be set to True."""
        metadata = SilverMetadata(
            sp_name="Test_SP",
            group_file_name="test/group.json",
            disabled=True
        )
        assert metadata.disabled is True
    
    def test_decorator_without_disabled_parameter(self):
        """Test decorator works without disabled parameter (defaults to False)."""
        @silver_metadata(
            sp_name="Load_Test_Dim",
            group_file_name="test/dimension.json"
        )
        class TestETL:
            pass
        
        assert hasattr(TestETL, '_silver_metadata')
        assert TestETL._silver_metadata.disabled is False
    
    def test_decorator_with_disabled_false(self):
        """Test decorator with disabled=False."""
        @silver_metadata(
            sp_name="Load_Test_Dim",
            group_file_name="test/dimension.json",
            disabled=False
        )
        class TestETL:
            pass
        
        assert hasattr(TestETL, '_silver_metadata')
        assert TestETL._silver_metadata.disabled is False
    
    def test_decorator_with_disabled_true(self):
        """Test decorator with disabled=True."""
        @silver_metadata(
            sp_name="Load_ClientSpecific_Dim",
            group_file_name="test/client_specific.json",
            disabled=True
        )
        class ClientSpecificETL:
            pass
        
        assert hasattr(ClientSpecificETL, '_silver_metadata')
        assert ClientSpecificETL._silver_metadata.disabled is True
    
    def test_metadata_discovery_extracts_disabled_field(self):
        """Test that metadata discovery correctly extracts the disabled field."""
        # Create a test class with disabled=True
        @silver_metadata(
            sp_name="Load_Disabled_Feature",
            group_file_name="test/disabled.json",
            disabled=True
        )
        class DisabledFeatureETL:
            pass
        
        discovery = SilverMetadataDiscovery()
        metadata = discovery._extract_metadata_from_class(DisabledFeatureETL)
        
        assert metadata is not None
        assert metadata.is_disabled is True
        assert metadata.sp_name == "Load_Disabled_Feature"
    
    def test_metadata_discovery_filters_disabled_transformations(self):
        """Test that disabled transformations are filtered out in discovery."""
        # Create test classes
        @silver_metadata(
            sp_name="Load_Enabled_Dim",
            group_file_name="test/enabled.json",
            disabled=False
        )
        class EnabledETL:
            pass
        
        @silver_metadata(
            sp_name="Load_Disabled_Dim",
            group_file_name="test/disabled.json",
            disabled=True
        )
        class DisabledETL:
            pass
        
        discovery = SilverMetadataDiscovery()
        
        # Add to cache manually for testing
        enabled_meta = discovery._extract_metadata_from_class(EnabledETL)
        disabled_meta = discovery._extract_metadata_from_class(DisabledETL)
        
        if enabled_meta:
            discovery._metadata_cache[enabled_meta.sp_name] = enabled_meta
        if disabled_meta:
            discovery._metadata_cache[disabled_meta.sp_name] = disabled_meta
        
        discovery._discovery_complete = True
        
        # Test get_transformations_by_model filters disabled
        enabled_meta.model_name = "sales"
        disabled_meta.model_name = "sales"
        
        transformations = discovery.get_transformations_by_model("sales")
        
        # Should only include enabled transformation
        assert len(transformations) == 1
        assert transformations[0].sp_name == "Load_Enabled_Dim"
        assert transformations[0].is_disabled is False
    
    def test_disabled_transformations_in_statistics(self):
        """Test that discovery statistics correctly count disabled transformations."""
        discovery = SilverMetadataDiscovery()
        
        # Create and add test metadata
        enabled_meta = TransformationMetadata(
            sp_name="Enabled_SP",
            model_name="test",
            silver_table_name="test_table",
            is_disabled=False
        )
        disabled_meta = TransformationMetadata(
            sp_name="Disabled_SP",
            model_name="test",
            silver_table_name="test_table2",
            is_disabled=True
        )
        
        discovery._metadata_cache = {
            "Enabled_SP": enabled_meta,
            "Disabled_SP": disabled_meta
        }
        discovery._discovery_complete = True
        
        stats = discovery.get_discovery_statistics()
        
        assert stats['total_transformations'] == 2
        assert stats['enabled_transformations'] == 1
        assert stats['disabled_transformations'] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])