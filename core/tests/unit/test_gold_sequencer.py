"""Unit tests for Gold Sequencer table filtering functionality."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from core.medallion.gold import GoldSequencer
from core.medallion.gold.decorators import gold_metadata
from core.medallion.base.decorators import query_metadata
from core.constants.sql import QueryType
from core.constants.medallion import Layer


@gold_metadata(
    schema_name="gold",
    description="Test Gold Sequencer"
)
class TestGoldSequencer(GoldSequencer):
    """Test implementation of Gold Sequencer with multiple views."""
    
    @query_metadata(
        type=QueryType.CREATE_OR_ALTER_VIEW,
        table_name="v_daily_sales",
        schema_name="gold"
    )
    def create_daily_sales(self) -> str:
        """Create daily sales view."""
        return "CREATE VIEW v_daily_sales AS SELECT * FROM silver.sales WHERE date_type = 'daily'"
    
    @query_metadata(
        type=QueryType.CREATE_OR_ALTER_VIEW,
        table_name="v_monthly_sales",
        schema_name="gold"
    )
    def create_monthly_sales(self) -> str:
        """Create monthly sales view."""
        return "CREATE VIEW v_monthly_sales AS SELECT * FROM silver.sales WHERE date_type = 'monthly'"
    
    @query_metadata(
        type=QueryType.CREATE_OR_ALTER_VIEW,
        table_name="v_yearly_sales",
        schema_name="gold"
    )
    def create_yearly_sales(self) -> str:
        """Create yearly sales view."""
        return "CREATE VIEW v_yearly_sales AS SELECT * FROM silver.sales WHERE date_type = 'yearly'"
    
    @query_metadata(
        type=QueryType.CREATE_TABLE,
        table_name="t_sales_summary",
        schema_name="gold"
    )
    def create_sales_summary(self) -> str:
        """Create sales summary table."""
        return "SELECT * FROM silver.sales_summary"


class TestGoldSequencerFiltering:
    """Test Gold Sequencer table filtering functionality."""
    
    @pytest.fixture
    def mock_settings(self):
        """Create mock settings for testing."""
        settings = Mock()
        settings.table_prefix = "test_"
        settings.compute.active_config.dialect = "sql"
        return settings
    
    @pytest.fixture
    def mock_feature_managers(self):
        """Mock feature managers to avoid initialization issues."""
        with patch('core.core.features.get_feature_manager') as mock_get_fm:
            mock_get_fm.return_value = None
            with patch('core.datalake.services.get_configuration_service') as mock_get_cs:
                mock_config_service = Mock()
                mock_config_service.initialize = Mock()
                mock_get_cs.return_value = mock_config_service
                yield
    
    @patch('core.medallion.gold.sequencer.get_settings')
    def test_gold_sequencer_no_filter(self, mock_get_settings, mock_settings, mock_feature_managers):
        """Test Gold Sequencer returns all queries when no filter is provided."""
        mock_get_settings.return_value = mock_settings
        
        sequencer = TestGoldSequencer()
        queries = sequencer.get_queries()
        
        # Should return all 4 operations
        assert len(queries) == 4
        
        # Verify all table names are present
        table_names = [op.object_name for op in queries]
        assert "v_daily_sales" in table_names
        assert "v_monthly_sales" in table_names
        assert "v_yearly_sales" in table_names
        assert "t_sales_summary" in table_names
    
    @patch('core.medallion.gold.sequencer.get_settings')
    def test_gold_sequencer_with_single_filter(self, mock_get_settings, mock_settings, mock_feature_managers):
        """Test Gold Sequencer filters to single table."""
        mock_get_settings.return_value = mock_settings
        
        sequencer = TestGoldSequencer(selected_tables=["v_daily_sales"])
        queries = sequencer.get_queries()
        
        # Should return only 1 operation
        assert len(queries) == 1
        assert queries[0].object_name == "v_daily_sales"
    
    @patch('core.medallion.gold.sequencer.get_settings')
    def test_gold_sequencer_with_multiple_filters(self, mock_get_settings, mock_settings, mock_feature_managers):
        """Test Gold Sequencer filters to multiple tables."""
        mock_get_settings.return_value = mock_settings
        
        sequencer = TestGoldSequencer(selected_tables=["v_daily_sales", "v_monthly_sales"])
        queries = sequencer.get_queries()
        
        # Should return 2 operations
        assert len(queries) == 2
        table_names = [op.object_name for op in queries]
        assert "v_daily_sales" in table_names
        assert "v_monthly_sales" in table_names
        assert "v_yearly_sales" not in table_names
        assert "t_sales_summary" not in table_names
    
    @patch('core.medallion.gold.sequencer.get_settings')
    def test_gold_sequencer_with_nonexistent_table(self, mock_get_settings, mock_settings, mock_feature_managers):
        """Test Gold Sequencer handles non-existent table names gracefully."""
        mock_get_settings.return_value = mock_settings
        
        sequencer = TestGoldSequencer(selected_tables=["v_nonexistent"])
        queries = sequencer.get_queries()
        
        # Should return empty list
        assert len(queries) == 0
    
    @patch('core.medallion.gold.sequencer.get_settings')
    def test_gold_sequencer_with_empty_list(self, mock_get_settings, mock_settings, mock_feature_managers):
        """Test Gold Sequencer handles empty selection list."""
        mock_get_settings.return_value = mock_settings
        
        sequencer = TestGoldSequencer(selected_tables=[])
        queries = sequencer.get_queries()
        
        # Should return empty list when explicitly given empty list
        assert len(queries) == 0
    
    @patch('core.medallion.gold.sequencer.get_settings')
    def test_gold_sequencer_mixed_query_types(self, mock_get_settings, mock_settings, mock_feature_managers):
        """Test Gold Sequencer correctly filters different query types."""
        mock_get_settings.return_value = mock_settings
        
        # Filter to include both VIEW and TABLE types
        sequencer = TestGoldSequencer(selected_tables=["v_daily_sales", "t_sales_summary"])
        queries = sequencer.get_queries()
        
        # Should return 2 operations
        assert len(queries) == 2
        table_names = [op.object_name for op in queries]
        assert "v_daily_sales" in table_names
        assert "t_sales_summary" in table_names
    
    @patch('core.medallion.gold.sequencer.get_settings')
    def test_gold_sequencer_preserves_layer_attribute(self, mock_get_settings, mock_settings, mock_feature_managers):
        """Test Gold Sequencer preserves layer attribute."""
        mock_get_settings.return_value = mock_settings
        
        sequencer = TestGoldSequencer(selected_tables=["v_daily_sales"])
        
        assert sequencer.layer == Layer.GOLD
        assert sequencer.get_layer_name() == "gold"
    
    @patch('core.medallion.gold.sequencer.get_settings')
    def test_gold_sequencer_backward_compatibility(self, mock_get_settings, mock_settings, mock_feature_managers):
        """Test Gold Sequencer maintains backward compatibility with no parameters."""
        mock_get_settings.return_value = mock_settings
        
        # Old style initialization without parameters
        sequencer = TestGoldSequencer()
        
        # Should still work and return all queries
        queries = sequencer.get_queries()
        assert len(queries) == 4