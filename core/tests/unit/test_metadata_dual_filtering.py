"""Test dual-filtering mechanism for metadata discovery.

Tests that transformations are filtered based on:
1. Transformation-level disabled flag
2. Model-level configuration
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from core.medallion.silver.metadata_discovery import SilverMetadataDiscovery, TransformationMetadata
from core.medallion.silver.decorators import silver_metadata
from core.medallion.silver.sequencer import SilverTransformationSequencer
from core.types import SilverMetadata


class TestMetadataDualFiltering:
    """Test the dual-filtering mechanism in metadata discovery."""
    
    @pytest.fixture
    def mock_settings(self):
        """Create mock settings with model configuration."""
        settings = Mock()
        settings.silver_package_name = "test_silver"
        # By default, only 'sales' and 'marketing' models are configured
        settings.is_model_configured = Mock(
            side_effect=lambda model: model in ['sales', 'marketing']
        )
        return settings
    
    @pytest.fixture
    def discovery_service(self, mock_settings):
        """Create discovery service with mocked settings."""
        with patch('core.medallion.silver.metadata_discovery.get_settings', return_value=mock_settings):
            with patch('core.medallion.silver.metadata_discovery.get_logger'):
                service = SilverMetadataDiscovery()
                return service
    
    def test_enabled_transformation_configured_model(self, discovery_service):
        """Test that enabled transformations with configured models are included."""
        
        @silver_metadata(
            sp_name="Load_Sales_Fact",
            group_file_name="group_sales/fact.json",
            disabled=False  # Enabled
        )
        class SalesFactETL(SilverTransformationSequencer):
            pass
        
        # Extract metadata
        metadata = discovery_service._extract_metadata_from_class(SalesFactETL)
        
        # Should be included (enabled + model configured)
        assert metadata is not None
        assert metadata.sp_name == "Load_Sales_Fact"
        assert metadata.model_name == "sales"
        assert metadata.sequencer_class is SalesFactETL
    
    def test_disabled_transformation_configured_model(self, discovery_service):
        """Test that disabled transformations are excluded even if model is configured."""
        
        @silver_metadata(
            sp_name="Load_Sales_Dim",
            group_file_name="group_sales/dim.json",
            disabled=True  # Disabled
        )
        class SalesDimETL(SilverTransformationSequencer):
            pass
        
        # Extract metadata
        metadata = discovery_service._extract_metadata_from_class(SalesDimETL)
        
        # Should be excluded (disabled=True)
        assert metadata is None
    
    def test_enabled_transformation_unconfigured_model(self, discovery_service):
        """Test that enabled transformations with unconfigured models are excluded."""
        
        @silver_metadata(
            sp_name="Load_Finance_Report",
            group_file_name="group_finance/report.json",
            disabled=False  # Enabled
        )
        class FinanceReportETL(SilverTransformationSequencer):
            pass
        
        # Extract metadata
        metadata = discovery_service._extract_metadata_from_class(FinanceReportETL)
        
        # Should be excluded (model 'finance' not configured)
        assert metadata is None
    
    def test_disabled_transformation_unconfigured_model(self, discovery_service):
        """Test that disabled transformations with unconfigured models are excluded."""
        
        @silver_metadata(
            sp_name="Load_HR_Employee",
            group_file_name="group_hr/employee.json",
            disabled=True  # Disabled
        )
        class HREmployeeETL(SilverTransformationSequencer):
            pass
        
        # Extract metadata
        metadata = discovery_service._extract_metadata_from_class(HREmployeeETL)
        
        # Should be excluded (both disabled AND model not configured)
        assert metadata is None
    
    def test_explicit_model_name_overrides_group(self, discovery_service):
        """Test that explicit model_name in metadata overrides group extraction."""
        
        @silver_metadata(
            sp_name="Load_Marketing_Campaign",
            group_file_name="group_old/campaign.json",  # Would extract 'old'
            model_name="marketing",  # Explicit override
            disabled=False
        )
        class MarketingCampaignETL(SilverTransformationSequencer):
            pass
        
        # Extract metadata
        metadata = discovery_service._extract_metadata_from_class(MarketingCampaignETL)
        
        # Should use explicit model_name
        assert metadata is not None
        assert metadata.model_name == "marketing"  # Not 'old'
    
    def test_logging_for_disabled_transformation(self, discovery_service):
        """Test that correct log message is generated for disabled transformations."""
        
        @silver_metadata(
            sp_name="Load_Disabled_Feature",
            group_file_name="group_sales/feature.json",
            disabled=True
        )
        class DisabledFeatureETL(SilverTransformationSequencer):
            pass
        
        with patch.object(discovery_service.logger, 'debug') as mock_log:
            metadata = discovery_service._extract_metadata_from_class(DisabledFeatureETL)
            
            assert metadata is None
            mock_log.assert_called_with("Skipping disabled transformation: Load_Disabled_Feature")
    
    def test_logging_for_unconfigured_model(self, discovery_service):
        """Test that correct log message is generated for unconfigured models."""
        
        @silver_metadata(
            sp_name="Load_Unconfigured_Table",
            group_file_name="group_finance/table.json",
            disabled=False
        )
        class UnconfiguredTableETL(SilverTransformationSequencer):
            pass
        
        with patch.object(discovery_service.logger, 'debug') as mock_log:
            metadata = discovery_service._extract_metadata_from_class(UnconfiguredTableETL)
            
            assert metadata is None
            mock_log.assert_called_with(
                "Skipping transformation Load_Unconfigured_Table: model 'finance' not configured"
            )
    
    def test_all_models_configured_scenario(self, mock_settings):
        """Test when all models are configured (permissive setting)."""
        
        # Configure to accept all models
        mock_settings.is_model_configured = Mock(return_value=True)
        
        with patch('core.medallion.silver.metadata_discovery.get_settings', return_value=mock_settings):
            with patch('core.medallion.silver.metadata_discovery.get_logger'):
                service = SilverMetadataDiscovery()
                
                @silver_metadata(
                    sp_name="Load_Any_Model",
                    group_file_name="group_anymodel/table.json",
                    disabled=False
                )
                class AnyModelETL(SilverTransformationSequencer):
                    pass
                
                # Should be included (all models configured)
                metadata = service._extract_metadata_from_class(AnyModelETL)
                assert metadata is not None
                assert metadata.model_name == "anymodel"
    
    def test_no_models_configured_scenario(self, mock_settings):
        """Test when no models are configured (restrictive setting)."""
        
        # Configure to reject all models
        mock_settings.is_model_configured = Mock(return_value=False)
        
        with patch('core.medallion.silver.metadata_discovery.get_settings', return_value=mock_settings):
            with patch('core.medallion.silver.metadata_discovery.get_logger'):
                service = SilverMetadataDiscovery()
                
                @silver_metadata(
                    sp_name="Load_Restricted_Model",
                    group_file_name="group_restricted/table.json",
                    disabled=False
                )
                class RestrictedModelETL(SilverTransformationSequencer):
                    pass
                
                # Should be excluded (no models configured)
                metadata = service._extract_metadata_from_class(RestrictedModelETL)
                assert metadata is None
    
    def test_filtering_preserves_metadata_integrity(self, discovery_service):
        """Test that metadata is properly preserved for included transformations."""
        
        @silver_metadata(
            sp_name="Load_Marketing_Customer",
            group_file_name="group_marketing/customer.json",
            description="Marketing customer data",
            tags=["marketing", "customer"],
            disabled=False
        )
        class MarketingCustomerETL(SilverTransformationSequencer):
            pass
        
        metadata = discovery_service._extract_metadata_from_class(MarketingCustomerETL)
        
        # Verify all metadata is preserved
        assert metadata is not None
        assert metadata.sp_name == "Load_Marketing_Customer"
        assert metadata.model_name == "marketing"
        assert metadata.sequencer_class is MarketingCustomerETL
        assert metadata.silver_metadata.description == "Marketing customer data"
        assert "marketing" in metadata.silver_metadata.tags
        assert "customer" in metadata.silver_metadata.tags
    
    def test_class_without_metadata_returns_none(self, discovery_service):
        """Test that classes without _silver_metadata return None."""
        
        class PlainClass:
            """Class without decorator."""
            pass
        
        metadata = discovery_service._extract_metadata_from_class(PlainClass)
        assert metadata is None
    
    def test_exception_handling_returns_none(self, discovery_service):
        """Test that exceptions during extraction return None and log error."""
        
        # Create a class with metadata that will cause an error
        class BadClass:
            _silver_metadata = Mock(
                sp_name="Bad_SP",
                disabled=False,
                model_name=None,
                group_file_name=None  # Will cause error in _extract_model_from_group
            )
        
        with patch.object(discovery_service.logger, 'error') as mock_error:
            metadata = discovery_service._extract_metadata_from_class(BadClass)
            
            assert metadata is None
            assert mock_error.called
            error_msg = mock_error.call_args[0][0]
            assert "Failed to extract metadata from BadClass" in error_msg


class TestIntegrationFiltering:
    """Integration tests for the complete discovery process with filtering."""
    
    def test_discover_all_with_mixed_transformations(self):
        """Test discovery with mix of enabled/disabled and configured/unconfigured."""
        
        # Create mock settings
        mock_settings = Mock()
        mock_settings.silver_package_name = "test_package"
        mock_settings.is_model_configured = Mock(
            side_effect=lambda model: model == 'sales'
        )
        
        # Create test transformations
        @silver_metadata(
            sp_name="Load_Sales_Included",
            group_file_name="group_sales/included.json",
            disabled=False
        )
        class SalesIncluded(SilverTransformationSequencer):
            pass
        
        @silver_metadata(
            sp_name="Load_Sales_Disabled",
            group_file_name="group_sales/disabled.json",
            disabled=True
        )
        class SalesDisabled(SilverTransformationSequencer):
            pass
        
        @silver_metadata(
            sp_name="Load_Finance_Enabled",
            group_file_name="group_finance/enabled.json",
            disabled=False
        )
        class FinanceEnabled(SilverTransformationSequencer):
            pass
        
        # Mock the discovery process
        with patch('core.medallion.silver.metadata_discovery.get_settings', return_value=mock_settings):
            with patch('core.medallion.silver.metadata_discovery.get_logger'):
                service = SilverMetadataDiscovery()
                
                # Mock the module walking to return our test classes
                with patch.object(service, '_walk_silver_package', return_value=[]):
                    with patch.object(service, '_extract_transformation_classes', 
                                    return_value=[SalesIncluded, SalesDisabled, FinanceEnabled]):
                        
                        # Test individual extraction
                        included = service._extract_metadata_from_class(SalesIncluded)
                        disabled = service._extract_metadata_from_class(SalesDisabled)
                        unconfigured = service._extract_metadata_from_class(FinanceEnabled)
                        
                        # Only SalesIncluded should be extracted
                        assert included is not None
                        assert included.sp_name == "Load_Sales_Included"
                        
                        assert disabled is None  # Disabled
                        assert unconfigured is None  # Model not configured


if __name__ == "__main__":
    pytest.main([__file__, "-v"])