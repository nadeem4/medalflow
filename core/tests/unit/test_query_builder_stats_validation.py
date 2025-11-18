"""Unit tests for query builder statistics validation."""

import pytest
from unittest.mock import Mock, MagicMock

from core.query_builder.base import BaseQueryBuilder
from core.query_builder.synapse.serverless_builder import SynapseServerlessQueryBuilder
from core.query_builder.fabric.warehouse_builder import FabricWarehouseQueryBuilder
from core.operations.statistics import CreateStatistics
from core.constants.sql import QueryType
from core.settings import _Settings


class TestQueryBuilderStatsValidation:
    """Test statistics validation in query builders."""
    
    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        settings = Mock()
        settings.table_prefix = "tbl"
        settings.compute = Mock()
        settings.compute.active_config = Mock()
        settings.compute.active_config.skip_prefix_on_schema = []
        settings.compute.active_config.dialect = "tsql"
        return settings
    
    def test_base_query_builder_validates_no_columns(self, mock_settings):
        """Test that base query builder validates when no columns are provided."""
        # Create a concrete implementation for testing
        class TestBuilder(BaseQueryBuilder):
            def _build_create_table(self, op): return "CREATE TABLE"
            def _build_drop_table(self, op): return "DROP TABLE"
            def _build_insert(self, op): return "INSERT"
            def _build_update(self, op): return "UPDATE"
            def _build_delete(self, op): return "DELETE"
            def _build_merge(self, op): return "MERGE"
            def _build_copy(self, op): return "COPY"
            def _build_create_or_alter_view(self, op): return "CREATE VIEW"
            def _build_drop_view(self, op): return "DROP VIEW"
            def _build_create_statistics(self, op): return "CREATE STATISTICS"
            def _build_create_schema(self, op): return "CREATE SCHEMA"
            def _build_drop_schema(self, op): return "DROP SCHEMA"
            def _build_select(self, op): return "SELECT"
            def _build_execute_sql(self, op): return op.sql_query
            def format_identifier(self, name): return name
            def quote_identifier(self, name): return f"[{name}]"
            def fully_qualified_name(self, schema, table): return f"{schema}.{table}"
            def format_column_definition(self, col): return f"{col.name} {col.data_type}"
            def format_column_list(self, cols): return ", ".join(cols)
            def validate_sql_statement(self, sql): return sql
        
        builder = TestBuilder(mock_settings)
        
        # Create stats operation without columns
        stats_op = Mock(spec=CreateStatistics)
        stats_op.operation_type = QueryType.CREATE_STATISTICS
        stats_op.schema_name = "bronze"
        stats_op.object_name = "test_table"
        stats_op.columns = None  # No columns
        
        with pytest.raises(ValueError, match="No columns specified"):
            builder.build_query(stats_op)
    
    def test_base_query_builder_validates_multiple_columns(self, mock_settings):
        """Test that base query builder rejects multiple columns."""
        # Create a concrete implementation
        class TestBuilder(BaseQueryBuilder):
            def _build_create_table(self, op): return "CREATE TABLE"
            def _build_drop_table(self, op): return "DROP TABLE"
            def _build_insert(self, op): return "INSERT"
            def _build_update(self, op): return "UPDATE"
            def _build_delete(self, op): return "DELETE"
            def _build_merge(self, op): return "MERGE"
            def _build_copy(self, op): return "COPY"
            def _build_create_or_alter_view(self, op): return "CREATE VIEW"
            def _build_drop_view(self, op): return "DROP VIEW"
            def _build_create_statistics(self, op): return "CREATE STATISTICS"
            def _build_create_schema(self, op): return "CREATE SCHEMA"
            def _build_drop_schema(self, op): return "DROP SCHEMA"
            def _build_select(self, op): return "SELECT"
            def _build_execute_sql(self, op): return op.sql_query
            def format_identifier(self, name): return name
            def quote_identifier(self, name): return f"[{name}]"
            def fully_qualified_name(self, schema, table): return f"{schema}.{table}"
            def format_column_definition(self, col): return f"{col.name} {col.data_type}"
            def format_column_list(self, cols): return ", ".join(cols)
            def validate_sql_statement(self, sql): return sql
        
        builder = TestBuilder(mock_settings)
        
        # Create stats operation with multiple columns
        stats_op = Mock(spec=CreateStatistics)
        stats_op.operation_type = QueryType.CREATE_STATISTICS
        stats_op.schema_name = "bronze"
        stats_op.object_name = "test_table"
        stats_op.columns = ["col1", "col2", "col3"]  # Multiple columns
        
        with pytest.raises(ValueError, match="Multiple columns specified.*only support single-column"):
            builder.build_query(stats_op)
    
    def test_serverless_builder_handles_single_column(self, mock_settings):
        """Test that Serverless builder correctly handles single column."""
        builder = SynapseServerlessQueryBuilder(mock_settings)
        
        # Create valid single-column stats operation
        stats_op = CreateStatistics(
            schema_name="bronze",
            object_name="test_table",
            columns=["customer_id"],
            stats_name="stat_customer",
            with_fullscan=True
        )
        
        sql = builder.build_query(stats_op)
        
        # Verify the SQL is generated correctly for single column
        assert "CREATE STATISTICS" in sql
        assert "[stat_customer]" in sql or "stat_customer" in sql
        assert "[customer_id]" in sql or "(customer_id)" in sql
        assert "WITH FULLSCAN" in sql
    
    def test_warehouse_builder_handles_single_column(self, mock_settings):
        """Test that Warehouse builder correctly handles single column."""
        builder = FabricWarehouseQueryBuilder(mock_settings)
        
        # Create valid single-column stats operation
        stats_op = CreateStatistics(
            schema_name="bronze",
            object_name="orders",
            columns=["order_id"],
            sample_percent=50.0,
            with_fullscan=False
        )
        
        sql = builder.build_query(stats_op)
        
        # Verify the SQL is generated correctly for single column
        assert "CREATE STATISTICS" in sql
        assert "order_id" in sql
        assert "WITH SAMPLE 50.0 PERCENT" in sql or "WITH SAMPLE 50 PERCENT" in sql
    
    def test_auto_generated_stats_name_single_column(self, mock_settings):
        """Test auto-generation of stats name for single column."""
        builder = SynapseServerlessQueryBuilder(mock_settings)
        
        # Create stats operation without explicit name
        stats_op = CreateStatistics(
            schema_name="silver",
            object_name="products",
            columns=["product_id"],
            stats_name=None,  # Let it auto-generate
            with_fullscan=True
        )
        
        sql = builder.build_query(stats_op)
        
        # Verify auto-generated name includes table and column
        assert "stat_products_product_id" in sql or "stat_tbl_products_product_id" in sql
    
    def test_error_message_includes_helpful_context(self, mock_settings):
        """Test that error messages provide helpful context."""
        # Create a concrete implementation
        class TestBuilder(BaseQueryBuilder):
            def _build_create_table(self, op): return "CREATE TABLE"
            def _build_drop_table(self, op): return "DROP TABLE"
            def _build_insert(self, op): return "INSERT"
            def _build_update(self, op): return "UPDATE"
            def _build_delete(self, op): return "DELETE"
            def _build_merge(self, op): return "MERGE"
            def _build_copy(self, op): return "COPY"
            def _build_create_or_alter_view(self, op): return "CREATE VIEW"
            def _build_drop_view(self, op): return "DROP VIEW"
            def _build_create_statistics(self, op): return "CREATE STATISTICS"
            def _build_create_schema(self, op): return "CREATE SCHEMA"
            def _build_drop_schema(self, op): return "DROP SCHEMA"
            def _build_select(self, op): return "SELECT"
            def _build_execute_sql(self, op): return op.sql_query
            def format_identifier(self, name): return name
            def quote_identifier(self, name): return f"[{name}]"
            def fully_qualified_name(self, schema, table): return f"{schema}.{table}"
            def format_column_definition(self, col): return f"{col.name} {col.data_type}"
            def format_column_list(self, cols): return ", ".join(cols)
            def validate_sql_statement(self, sql): return sql
        
        builder = TestBuilder(mock_settings)
        
        # Test with multiple columns
        stats_op = Mock(spec=CreateStatistics)
        stats_op.operation_type = QueryType.CREATE_STATISTICS
        stats_op.schema_name = "gold"
        stats_op.object_name = "fact_sales"
        stats_op.full_object_name = "gold.fact_sales"  # Add the property that's used in error message
        stats_op.columns = ["date_id", "product_id", "customer_id"]
        
        with pytest.raises(ValueError) as exc_info:
            builder.build_query(stats_op)
        
        error_msg = str(exc_info.value)
        # Verify error includes all important context
        assert "gold.fact_sales" in error_msg
        assert "date_id, product_id, customer_id" in error_msg
        assert "Create separate statistics for each column" in error_msg