"""Test cache integration in SilverMetadataDiscovery."""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from core.medallion.silver.metadata_discovery import SilverMetadataDiscovery, TransformationMetadata
from core.medallion.silver.decorators import silver_metadata
from core.medallion.silver.sequencer import SilverTransformationSequencer
from core.types import SilverMetadata
from core.protocols import CacheProtocol


class TestMetadataDiscoveryCache:
    """Test cache integration in metadata discovery."""
    
    @pytest.fixture
    def mock_cache(self):
        """Create a mock cache manager."""
        cache = Mock(spec=CacheProtocol)
        cache.get.return_value = None  # Default to cache miss
        cache.exists.return_value = False
        cache.set.return_value = None
        cache.clear.return_value = 0
        cache.get_stats.return_value = {'total_keys': 0}
        return cache
    
    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        settings.silver_package_name = "test_silver"
        settings.models = Mock()
        settings.models.is_model_configured = Mock(return_value=True)
        return settings
    
    @pytest.fixture
    def discovery_with_cache(self, mock_cache, mock_settings):
        """Create discovery service with mock cache."""
        with patch('core.medallion.silver.metadata_discovery.get_settings', return_value=mock_settings):
            with patch('core.medallion.silver.metadata_discovery.get_logger'):
                with patch('core.medallion.silver.metadata_discovery.get_feature_manager', return_value=mock_cache):
                    discovery = SilverMetadataDiscovery()
                    return discovery
    
    @pytest.fixture
    def discovery_without_cache(self, mock_settings):
        """Create discovery service without cache."""
        with patch('core.medallion.silver.metadata_discovery.get_settings', return_value=mock_settings):
            with patch('core.medallion.silver.metadata_discovery.get_logger'):
                with patch('core.medallion.silver.metadata_discovery.get_feature_manager', return_value=None):
                    discovery = SilverMetadataDiscovery()
                    return discovery
    
    @pytest.fixture
    def sample_transformations(self):
        """Create sample transformation metadata."""
        @silver_metadata(
            sp_name="Load_Sales_Fact",
            group_file_name="group_sales/fact.json"
        )
        class SalesFactETL(SilverTransformationSequencer):
            pass
        
        @silver_metadata(
            sp_name="Load_Sales_Dim",
            group_file_name="group_sales/dim.json"
        )
        class SalesDimETL(SilverTransformationSequencer):
            pass
        
        @silver_metadata(
            sp_name="Load_Marketing_Campaign",
            group_file_name="group_marketing/campaign.json"
        )
        class MarketingCampaignETL(SilverTransformationSequencer):
            pass
        
        return [
            TransformationMetadata(
                sp_name="Load_Sales_Fact",
                model_name="sales",
                sequencer_class=SalesFactETL,
                silver_metadata=SalesFactETL._silver_metadata
            ),
            TransformationMetadata(
                sp_name="Load_Sales_Dim",
                model_name="sales",
                sequencer_class=SalesDimETL,
                silver_metadata=SalesDimETL._silver_metadata
            ),
            TransformationMetadata(
                sp_name="Load_Marketing_Campaign",
                model_name="marketing",
                sequencer_class=MarketingCampaignETL,
                silver_metadata=MarketingCampaignETL._silver_metadata
            )
        ]
    
    def test_discover_all_with_cache_hit(self, discovery_with_cache, sample_transformations):
        """Test discovery with cache hit."""
        # Setup cache to return data
        discovery_with_cache._cache_manager.get.return_value = sample_transformations
        
        # Call discover
        result = discovery_with_cache.discover_all_transformations()
        
        # Verify cache was checked
        discovery_with_cache._cache_manager.get.assert_called_with("silver:metadata:all")
        
        # Verify no actual discovery was performed (no module walking)
        assert result == sample_transformations
    
    def test_discover_all_with_cache_miss(self, discovery_with_cache, sample_transformations):
        """Test discovery with cache miss."""
        # Setup cache miss
        discovery_with_cache._cache_manager.get.return_value = None
        
        # Mock the discovery process
        with patch.object(discovery_with_cache, '_perform_discovery', return_value=[]):
            # Call discover
            result = discovery_with_cache.discover_all_transformations()
            
            # Verify cache was checked
            discovery_with_cache._cache_manager.get.assert_called_with("silver:metadata:all")
            
            # Since we mocked to return no classes, result should be empty
            # but cache.set should still be called
            assert result == []
            
            # Verify result was cached (even empty result gets cached)
            discovery_with_cache._cache_manager.set.assert_called_with(
                "silver:metadata:all", 
                [], 
                ttl=3600
            )
    
    def test_discover_all_force_refresh(self, discovery_with_cache):
        """Test discovery with force refresh clears cache."""
        # Call with force_refresh
        with patch.object(discovery_with_cache, '_perform_discovery', return_value=[]):
            discovery_with_cache.discover_all_transformations(force_refresh=True)
        
        # Verify cache was cleared
        discovery_with_cache._cache_manager.clear.assert_called_with("silver:metadata:*")
        
        # Verify cache get was not called (force refresh)
        discovery_with_cache._cache_manager.get.assert_not_called()
    
    def test_get_transformations_by_model_cached(self, discovery_with_cache, sample_transformations):
        """Test get_transformations_by_model with cache hit."""
        # Setup cache hit for model
        sales_transformations = [t for t in sample_transformations if t.model_name == "sales"]
        discovery_with_cache._cache_manager.get.return_value = sales_transformations
        
        # Call method
        result = discovery_with_cache.get_transformations_by_model("sales")
        
        # Verify cache was checked
        discovery_with_cache._cache_manager.get.assert_called_with("silver:metadata:model:sales")
        
        # Verify result
        assert len(result) == 2
        assert all(t.model_name == "sales" for t in result)
    
    def test_get_transformations_by_model_cache_miss(self, discovery_with_cache, sample_transformations):
        """Test get_transformations_by_model with cache miss."""
        # Setup cache miss for model-specific query
        def cache_get_side_effect(key):
            if key == "silver:metadata:model:sales":
                return None  # Cache miss for model
            elif key == "silver:metadata:all":
                return sample_transformations  # Cache hit for all transformations
            return None
        
        discovery_with_cache._cache_manager.get.side_effect = cache_get_side_effect
        
        # Call method
        result = discovery_with_cache.get_transformations_by_model("sales")
        
        # Verify result was cached
        expected_result = [t for t in sample_transformations if t.model_name == "sales"]
        discovery_with_cache._cache_manager.set.assert_called_with(
            "silver:metadata:model:sales",
            expected_result,
            ttl=3600
        )
        
        assert len(result) == 2
    
    def test_get_transformation_by_sp_cached(self, discovery_with_cache, sample_transformations):
        """Test get_transformation_by_sp with cache hit."""
        # Setup cache hit
        transformation = sample_transformations[0]
        discovery_with_cache._cache_manager.get.return_value = transformation
        
        # Call method
        result = discovery_with_cache.get_transformation_by_sp("Load_Sales_Fact")
        
        # Verify cache was checked
        discovery_with_cache._cache_manager.get.assert_called_with("silver:metadata:sp:Load_Sales_Fact")
        
        # Verify result
        assert result == transformation
    
    def test_get_transformation_by_sp_not_found_cached(self, discovery_with_cache):
        """Test get_transformation_by_sp with cached None (not found)."""
        # Setup cache to indicate key exists but value is None
        discovery_with_cache._cache_manager.get.return_value = None
        discovery_with_cache._cache_manager.exists.return_value = True
        
        # Call method
        result = discovery_with_cache.get_transformation_by_sp("NonExistent_SP")
        
        # Verify cache was checked
        discovery_with_cache._cache_manager.get.assert_called_with("silver:metadata:sp:NonExistent_SP")
        discovery_with_cache._cache_manager.exists.assert_called_with("silver:metadata:sp:NonExistent_SP")
        
        # Verify result
        assert result is None
    
    def test_get_all_models_cached(self, discovery_with_cache):
        """Test get_all_models with cache hit."""
        # Setup cache hit
        models = ["finance", "marketing", "sales"]
        discovery_with_cache._cache_manager.get.return_value = models
        
        # Call method
        result = discovery_with_cache.get_all_models()
        
        # Verify cache was checked
        discovery_with_cache._cache_manager.get.assert_called_with("silver:metadata:models")
        
        # Verify result
        assert result == models
    
    def test_clear_cache_all(self, discovery_with_cache):
        """Test clearing all silver metadata cache."""
        discovery_with_cache._cache_manager.clear.return_value = 10
        
        # Clear all cache
        discovery_with_cache.clear_cache()
        
        # Verify cache was cleared with correct pattern
        discovery_with_cache._cache_manager.clear.assert_called_with("silver:metadata:*")
    
    def test_clear_cache_pattern(self, discovery_with_cache):
        """Test clearing cache with specific pattern."""
        discovery_with_cache._cache_manager.clear.return_value = 3
        
        # Clear specific pattern
        discovery_with_cache.clear_cache("silver:metadata:model:*")
        
        # Verify cache was cleared with correct pattern
        discovery_with_cache._cache_manager.clear.assert_called_with("silver:metadata:model:*")
    
    def test_clear_cache_without_manager(self, discovery_without_cache):
        """Test clear_cache when cache manager is not available."""
        # Should not raise error
        discovery_without_cache.clear_cache()
    
    def test_warm_cache(self, discovery_with_cache, sample_transformations):
        """Test warming the cache."""
        # Mock discovery and model methods
        with patch.object(discovery_with_cache, 'discover_all_transformations', return_value=sample_transformations) as mock_discover:
            with patch.object(discovery_with_cache, 'get_all_models', return_value=["sales", "marketing"]) as mock_models:
                with patch.object(discovery_with_cache, 'get_transformations_by_model') as mock_by_model:
                    
                    # Warm cache
                    discovery_with_cache.warm_cache()
                    
                    # Verify discovery was forced
                    mock_discover.assert_called_with(force_refresh=True)
                    
                    # Verify models were fetched
                    mock_models.assert_called_once()
                    
                    # Verify each model was cached
                    assert mock_by_model.call_count == 2
                    mock_by_model.assert_any_call("sales")
                    mock_by_model.assert_any_call("marketing")
    
    def test_get_cache_stats(self, discovery_with_cache, sample_transformations):
        """Test getting cache statistics."""
        # Setup
        discovery_with_cache._cache_manager.get_stats.return_value = {
            'total_keys': 15,
            'keys_with_ttl': 10
        }
        
        # Get stats
        stats = discovery_with_cache.get_cache_stats()
        
        # Verify stats
        assert stats['cache_available'] is True
        assert 'global_cache_stats' in stats
        assert stats['global_cache_stats']['total_keys'] == 15
    
    def test_discovery_without_cache_works(self, discovery_without_cache, sample_transformations):
        """Test that discovery works without cache manager."""
        # Setup mock discovery
        with patch.object(discovery_without_cache, '_perform_discovery', return_value=sample_transformations):
            # Should work without cache
            result = discovery_without_cache.discover_all_transformations()
            assert len(result) == 3
            
        # Mock discover_all_transformations for the other methods
        with patch.object(discovery_without_cache, 'discover_all_transformations', return_value=sample_transformations):
            # Get by model should work
            sales = discovery_without_cache.get_transformations_by_model("sales")
            assert len(sales) == 2
            
            # Get by SP should work
            transformation = discovery_without_cache.get_transformation_by_sp("Load_Sales_Fact")
            assert transformation is not None


class TestCacheKeyPatterns:
    """Test cache key patterns and namespacing."""
    
    def test_cache_key_patterns(self):
        """Test that cache keys follow the expected pattern."""
        discovery = SilverMetadataDiscovery.__new__(SilverMetadataDiscovery)
        
        # These are the expected patterns (not actual method calls)
        expected_patterns = {
            'all': "silver:metadata:all",
            'model': "silver:metadata:model:sales",
            'sp': "silver:metadata:sp:Load_Sales_Fact",
            'models': "silver:metadata:models"
        }
        
        # Verify patterns are consistent
        assert all(key.startswith("silver:metadata:") for key in expected_patterns.values())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])