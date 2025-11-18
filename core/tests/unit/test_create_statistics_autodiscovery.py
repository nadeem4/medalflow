"""Unit tests for CreateStatistics auto-discovery feature."""

import pytest
from unittest.mock import Mock, patch

from core.operations.statistics import CreateStatistics
from core.constants.sql import QueryType


class TestCreateStatisticsAutoDiscovery:
    """Test CreateStatistics column auto-discovery functionality."""
    
    def test_create_statistics_with_explicit_columns(self):
        """Test CreateStatistics works normally with explicit columns."""
        stats_op = CreateStatistics(
            schema_name="bronze",
            object_name="test_table",
            columns=["col1", "col2"],
            stats_name="test_stats"
        )
        
        assert stats_op.columns == ["col1", "col2"]
        assert stats_op.stats_name == "test_stats"
        assert stats_op.operation_type == QueryType.CREATE_STATISTICS
        assert stats_op.auto_discover is False
    
    def test_create_statistics_fails_without_columns_and_no_autodiscover(self):
        """Test CreateStatistics fails when no columns and auto_discover=False."""
        with pytest.raises(ValueError, match="No columns specified for statistics"):
            CreateStatistics(
                schema_name="bronze",
                object_name="test_table",
                auto_discover=False
            )
    
    @patch('core.core.features.get_feature_manager')
    def test_auto_discover_finds_columns(self, mock_get_feature_manager):
        """Test auto-discovery successfully finds columns via StatsManager."""
        # Setup mock StatsManager
        mock_stats_mgr = Mock()
        mock_stats_mgr.get_stats_columns.return_value = ["col1", "col2", "col3"]
        mock_get_feature_manager.return_value = mock_stats_mgr
        
        # Create stats operation with auto-discovery
        stats_op = CreateStatistics(
            schema_name="bronze",
            object_name="test_table",
            auto_discover=True
        )
        
        # Verify columns were discovered
        assert stats_op.columns == ["col1", "col2", "col3"]
        mock_stats_mgr.get_stats_columns.assert_called_once_with(
            table_name="test_table",
            layer="bronze"
        )
    
    @patch('core.core.features.get_feature_manager')
    def test_auto_discover_uses_table_name_override(self, mock_get_feature_manager):
        """Test auto-discovery uses table_name field when provided."""
        # Setup mock StatsManager
        mock_stats_mgr = Mock()
        mock_stats_mgr.get_stats_columns.return_value = ["col1"]
        mock_get_feature_manager.return_value = mock_stats_mgr
        
        # Create stats operation with custom table_name
        stats_op = CreateStatistics(
            schema_name="bronze",
            object_name="stats_object",
            table_name="actual_table",
            auto_discover=True
        )
        
        # Verify table_name was used for discovery
        mock_stats_mgr.get_stats_columns.assert_called_once_with(
            table_name="actual_table",
            layer="bronze"
        )
        assert stats_op.columns == ["col1"]
    
    @patch('core.core.features.get_feature_manager')
    def test_auto_discover_fails_when_no_columns_found(self, mock_get_feature_manager):
        """Test auto-discovery fails gracefully when no columns found."""
        # Setup mock StatsManager that returns None
        mock_stats_mgr = Mock()
        mock_stats_mgr.get_stats_columns.return_value = None
        mock_get_feature_manager.return_value = mock_stats_mgr
        
        # Should raise error when no columns discovered
        with pytest.raises(ValueError, match="No columns specified for statistics"):
            CreateStatistics(
                schema_name="bronze",
                object_name="test_table",
                auto_discover=True
            )
    
    @patch('core.core.features.get_feature_manager')
    def test_auto_discover_handles_stats_manager_not_available(self, mock_get_feature_manager):
        """Test auto-discovery handles StatsManager not being available."""
        # StatsManager not available
        mock_get_feature_manager.return_value = None
        
        # Should raise error when StatsManager not available
        with pytest.raises(ValueError, match="No columns specified for statistics"):
            CreateStatistics(
                schema_name="bronze",
                object_name="test_table",
                auto_discover=True
            )
    
    @patch('core.core.features.get_feature_manager')
    def test_auto_discover_handles_exception(self, mock_get_feature_manager):
        """Test auto-discovery handles exceptions gracefully."""
        # Setup mock that raises exception
        mock_get_feature_manager.side_effect = Exception("Feature manager error")
        
        # Should handle exception and raise ValueError
        with pytest.raises(ValueError, match="No columns specified for statistics"):
            CreateStatistics(
                schema_name="bronze",
                object_name="test_table",
                auto_discover=True
            )
    
    @patch('core.core.features.get_feature_manager')
    def test_explicit_columns_override_auto_discover(self, mock_get_feature_manager):
        """Test explicit columns take precedence over auto-discovery."""
        # Setup mock (should not be called)
        mock_stats_mgr = Mock()
        mock_get_feature_manager.return_value = mock_stats_mgr
        
        # Create with both explicit columns and auto_discover
        stats_op = CreateStatistics(
            schema_name="bronze",
            object_name="test_table",
            columns=["explicit_col"],
            auto_discover=True
        )
        
        # Should use explicit columns, not call StatsManager
        assert stats_op.columns == ["explicit_col"]
        mock_stats_mgr.get_stats_columns.assert_not_called()
    
    def test_validate_sampling_options(self):
        """Test sampling validation still works with auto-discovery."""
        # Should not allow both sample_percent and with_fullscan
        with pytest.raises(ValueError, match="Cannot specify both sample_percent and with_fullscan"):
            CreateStatistics(
                schema_name="bronze",
                object_name="test_table",
                columns=["col1"],
                sample_percent=50.0,
                with_fullscan=True
            )
        
        # Should allow sample_percent without fullscan
        stats_op = CreateStatistics(
            schema_name="bronze",
            object_name="test_table",
            columns=["col1"],
            sample_percent=50.0,
            with_fullscan=False
        )
        assert stats_op.sample_percent == 50.0
        assert stats_op.with_fullscan is False