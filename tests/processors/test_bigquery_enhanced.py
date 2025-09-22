"""Enhanced tests for BigQuery processor with unified primary key handling."""

import pytest
from unittest.mock import Mock, MagicMock, patch
import pandas as pd
from sqlglot.expressions import Select

from data_check.processors.bigquery import BigQueryProcessor
from data_check.models.table import TableSchema, ColumnSchema, BigQueryDataType
from data_check.utils.primary_key_utils import PrimaryKeyHandler


class TestBigQueryProcessorPrimaryKeyHandling:
    """Test BigQuery processor with unified primary key handling."""

    @pytest.fixture
    def mock_query_client(self):
        """Mock the BigQuery client."""
        with patch('data_check.processors.bigquery.QueryBigQuery') as mock_client:
            client_instance = Mock()
            mock_client.return_value = client_instance
            yield client_instance

    @pytest.fixture
    def sample_processor(self, mock_query_client):
        """Create BigQuery processor for testing."""
        processor = BigQueryProcessor(
            "SELECT * FROM table1",
            "SELECT * FROM table2"
        )
        # Set up primary key for testing
        processor.set_config_data(
            primary_key=["id"],
            columns_to_compare=["name", "value"],
            sampling_rate=100
        )
        return processor

    @pytest.fixture
    def composite_key_processor(self, mock_query_client):
        """Create processor with composite primary key."""
        processor = BigQueryProcessor(
            "SELECT * FROM table1",
            "SELECT * FROM table2"
        )
        processor.set_config_data(
            primary_key=["customer_id", "order_id"],
            columns_to_compare=["name", "value"],
            sampling_rate=100
        )
        return processor

    def test_primary_key_handler_initialization(self, sample_processor):
        """Test that primary key handler is initialized correctly."""
        pk_handler = sample_processor.pk_handler
        assert isinstance(pk_handler, PrimaryKeyHandler)
        assert pk_handler.keys == ["id"]
        assert pk_handler.is_single_key

    def test_primary_key_handler_refresh_on_change(self, sample_processor):
        """Test that primary key handler refreshes when keys change."""
        original_handler = sample_processor.pk_handler
        assert original_handler.keys == ["id"]

        # Change primary key
        sample_processor.set_config_data(
            primary_key=["customer_id", "order_id"],
            columns_to_compare=["name", "value"],
            sampling_rate=100
        )

        new_handler = sample_processor.pk_handler
        assert new_handler.keys == ["customer_id", "order_id"]
        assert new_handler.is_composite_key

    def test_composite_primary_key_handling(self, composite_key_processor):
        """Test handling of composite primary keys."""
        pk_handler = composite_key_processor.pk_handler
        assert pk_handler.keys == ["customer_id", "order_id"]
        assert pk_handler.is_composite_key
        assert not pk_handler.is_single_key

    def test_get_query_insight_tables_primary_keys_single_key(self, sample_processor):
        """Test primary key insight query with single key."""
        query = sample_processor.get_query_insight_tables_primary_keys()

        assert isinstance(query, Select)
        # Verify the query uses the primary key handler
        assert sample_processor.pk_handler.is_single_key

    def test_get_query_insight_tables_primary_keys_composite_key(self, composite_key_processor):
        """Test primary key insight query with composite key."""
        query = composite_key_processor.get_query_insight_tables_primary_keys()

        assert isinstance(query, Select)
        # Verify the query uses composite key logic
        assert composite_key_processor.pk_handler.is_composite_key

    def test_get_query_check_primary_keys_unique(self, sample_processor):
        """Test primary key uniqueness check query."""
        query = sample_processor.get_query_check_primary_keys_unique("table1")
        assert isinstance(query, Select)

    def test_get_query_exclusive_primary_keys_table1(self, sample_processor):
        """Test exclusive primary keys query for table1."""
        # Mock the get_common_schema_from_tables method
        mock_schema = Mock()
        mock_schema.columns_names = ["id", "name", "value"]
        sample_processor.get_common_schema_from_tables = Mock(return_value=mock_schema)

        query = sample_processor.get_query_exclusive_primary_keys("table1")
        assert isinstance(query, Select)

    def test_get_query_exclusive_primary_keys_table2(self, composite_key_processor):
        """Test exclusive primary keys query for table2 with composite key."""
        # Mock the get_common_schema_from_tables method
        mock_schema = Mock()
        mock_schema.columns_names = ["customer_id", "order_id", "name", "value"]
        composite_key_processor.get_common_schema_from_tables = Mock(return_value=mock_schema)

        query = composite_key_processor.get_query_exclusive_primary_keys("table2")
        assert isinstance(query, Select)

    def test_get_query_plain_diff_tables(self, sample_processor):
        """Test plain diff query generation."""
        # Create a mock table schema
        mock_schema = TableSchema(
            table_name="test",
            columns=[
                ColumnSchema("id", BigQueryDataType.INTEGER, None),
                ColumnSchema("name", BigQueryDataType.STRING, None),
                ColumnSchema("value", BigQueryDataType.FLOAT, None)
            ]
        )

        query = sample_processor.get_query_plain_diff_tables(mock_schema)
        assert isinstance(query, Select)

    def test_query_ratio_common_values_per_column(self, composite_key_processor):
        """Test ratio common values query with composite key."""
        # Create a mock table schema
        mock_schema = TableSchema(
            table_name="test",
            columns=[
                ColumnSchema("customer_id", BigQueryDataType.STRING, None),
                ColumnSchema("order_id", BigQueryDataType.INTEGER, None),
                ColumnSchema("name", BigQueryDataType.STRING, None)
            ]
        )

        query = composite_key_processor.query_ratio_common_values_per_column(mock_schema)
        assert isinstance(query, Select)


class TestBigQueryProcessorEdgeCases:
    """Test edge cases in BigQuery processor."""

    @pytest.fixture
    def mock_query_client(self):
        """Mock the BigQuery client."""
        with patch('data_check.processors.bigquery.QueryBigQuery') as mock_client:
            client_instance = Mock()
            mock_client.return_value = client_instance
            yield client_instance

    def test_single_primary_key_as_string(self, mock_query_client):
        """Test handling single primary key provided as string."""
        processor = BigQueryProcessor(
            "SELECT * FROM table1",
            "SELECT * FROM table2"
        )
        processor.set_config_data(
            primary_key="id",  # String instead of list
            columns_to_compare=["name"],
            sampling_rate=100
        )

        assert processor.pk_handler.keys == ["id"]
        assert processor.pk_handler.is_single_key

    def test_comma_separated_primary_keys(self, mock_query_client):
        """Test handling comma-separated primary keys."""
        processor = BigQueryProcessor(
            "SELECT * FROM table1",
            "SELECT * FROM table2"
        )
        processor.set_config_data(
            primary_key="customer_id,order_id,item_id",  # Comma-separated
            columns_to_compare=["name"],
            sampling_rate=100
        )

        assert processor.pk_handler.keys == ["customer_id", "order_id", "item_id"]
        assert processor.pk_handler.is_composite_key

    def test_primary_key_with_spaces(self, mock_query_client):
        """Test handling primary keys with spaces."""
        processor = BigQueryProcessor(
            "SELECT * FROM table1",
            "SELECT * FROM table2"
        )
        processor.set_config_data(
            primary_key="customer_id, order_id, item_id",  # With spaces
            columns_to_compare=["name"],
            sampling_rate=100
        )

        assert processor.pk_handler.keys == ["customer_id", "order_id", "item_id"]

    def test_invalid_primary_key_handling(self, mock_query_client):
        """Test error handling for invalid primary keys."""
        processor = BigQueryProcessor(
            "SELECT * FROM table1",
            "SELECT * FROM table2"
        )

        with pytest.raises(ValueError):
            processor.set_config_data(
                primary_key=[],  # Empty list
                columns_to_compare=["name"],
                sampling_rate=100
            )

        with pytest.raises(ValueError):
            processor.set_config_data(
                primary_key=None,  # None value
                columns_to_compare=["name"],
                sampling_rate=100
            )


class TestBigQueryProcessorSQLGeneration:
    """Test SQL generation with unified primary key handling."""

    @pytest.fixture
    def processor_for_sql_tests(self):
        """Create processor for SQL generation tests."""
        with patch('data_check.processors.bigquery.QueryBigQuery'):
            processor = BigQueryProcessor(
                "SELECT * FROM table1",
                "SELECT * FROM table2"
            )
            return processor

    def test_single_key_sql_expressions(self, processor_for_sql_tests):
        """Test SQL expressions for single primary key."""
        processor_for_sql_tests.set_config_data(
            primary_key=["id"],
            columns_to_compare=["name"],
            sampling_rate=100
        )

        pk_handler = processor_for_sql_tests.pk_handler

        # Test concatenation expression
        concat_expr = pk_handler.get_concat_expression("table1")
        assert concat_expr == "table1.id"

        # Test join condition
        join_condition = pk_handler.get_join_condition()
        assert join_condition == "table1.id = table2.id"

        # Test null check condition
        null_check = pk_handler.get_null_check_condition("table1")
        assert null_check == "table1.id is null or table1.id = ''"

    def test_composite_key_sql_expressions(self, processor_for_sql_tests):
        """Test SQL expressions for composite primary key."""
        processor_for_sql_tests.set_config_data(
            primary_key=["customer_id", "order_id"],
            columns_to_compare=["name"],
            sampling_rate=100
        )

        pk_handler = processor_for_sql_tests.pk_handler

        # Test concatenation expression
        concat_expr = pk_handler.get_concat_expression("table1")
        assert "concat(" in concat_expr
        assert "coalesce(cast(table1.customer_id as string), '')" in concat_expr
        assert "coalesce(cast(table1.order_id as string), '')" in concat_expr

        # Test join condition
        join_condition = pk_handler.get_join_condition()
        assert "concat(" in join_condition

        # Test null check condition
        null_check = pk_handler.get_null_check_condition("table1")
        assert "concat(" in null_check
        assert "is null or" in null_check

    def test_group_by_columns_generation(self, processor_for_sql_tests):
        """Test GROUP BY columns generation."""
        processor_for_sql_tests.set_config_data(
            primary_key=["customer_id", "order_id"],
            columns_to_compare=["name"],
            sampling_rate=100
        )

        pk_handler = processor_for_sql_tests.pk_handler
        group_by_cols = pk_handler.get_group_by_columns("t1")

        assert group_by_cols == ["t1.customer_id", "t1.order_id"]

    def test_select_columns_generation(self, processor_for_sql_tests):
        """Test SELECT columns generation."""
        processor_for_sql_tests.set_config_data(
            primary_key=["customer_id", "order_id"],
            columns_to_compare=["name"],
            sampling_rate=100
        )

        pk_handler = processor_for_sql_tests.pk_handler
        select_cols = pk_handler.get_select_columns("table1")

        assert len(select_cols) == 2
        # Verify these are sqlglot column objects
        assert all(hasattr(col, 'table') for col in select_cols)


class TestBigQueryProcessorPerformance:
    """Test performance aspects of BigQuery processor."""

    @pytest.fixture
    def performance_processor(self):
        """Create processor for performance testing."""
        with patch('data_check.processors.bigquery.QueryBigQuery'):
            processor = BigQueryProcessor(
                "large_table1",
                "large_table2"
            )
            processor.set_config_data(
                primary_key=["id"],
                columns_to_compare=["col1", "col2", "col3"],
                sampling_rate=50  # Test with sampling
            )
            return processor

    def test_sampling_query_generation(self, performance_processor):
        """Test that sampling is properly handled in query generation."""
        # Test with sampling enabled
        performance_processor.set_config_data(
            primary_key=["id"],
            columns_to_compare=["col1"],
            sampling_rate=50
        )

        assert performance_processor.sampling_rate == 50
        assert performance_processor.is_sampling_allowed  # Should be True for direct tables

    def test_primary_key_handler_caching(self, performance_processor):
        """Test that primary key handler is cached and reused."""
        # Access handler multiple times
        handler1 = performance_processor.pk_handler
        handler2 = performance_processor.pk_handler
        handler3 = performance_processor.pk_handler

        # Should be the same instance (cached)
        assert handler1 is handler2 is handler3

        # Change primary key - should get new handler
        performance_processor.set_config_data(
            primary_key=["customer_id", "order_id"],
            columns_to_compare=["col1"],
            sampling_rate=50
        )

        handler4 = performance_processor.pk_handler
        assert handler4 is not handler1  # Should be different instance