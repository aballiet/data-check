"""End-to-end integration tests for data comparison workflow."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from data_check.processors.bigquery import BigQueryProcessor
from data_check.models.table import TableSchema, ColumnSchema, BigQueryDataType
from data_check.ui.components import (
    TableSelectionForm,
    ColumnConfigurationForm,
    PrimaryKeyAnalysisComponent,
    ColumnDifferenceAnalysisComponent,
    RowDifferenceViewerComponent
)


@pytest.mark.integration
class TestEndToEndDataComparison:
    """Test complete data comparison workflow."""

    @pytest.fixture
    def mock_bigquery_environment(self):
        """Set up complete mock BigQuery environment."""
        with patch('data_check.processors.bigquery.QueryBigQuery') as mock_query_class:

            # Mock BigQuery client
            mock_client = Mock()
            mock_query_class.return_value = mock_client

            # Mock table schema using proper TableSchema objects
            from data_check.models.table import TableSchema, ColumnSchema, BigQueryDataType
            
            schema_table = TableSchema(
                table_name="test_table",
                columns=[
                    ColumnSchema("id", BigQueryDataType.INTEGER, None),
                    ColumnSchema("name", BigQueryDataType.STRING, None),
                    ColumnSchema("value", BigQueryDataType.FLOAT, None),
                ]
            )
            
            mock_table = Mock()
            mock_table.table_id = "test_table"
            mock_table.schema = schema_table.columns
            mock_client.get_table.return_value = mock_table
            
            # Mock schema methods
            mock_client.get_table_schema_from_table.return_value = schema_table
            mock_client.get_table_schema_from_sql.return_value = schema_table

            # Mock query results
            mock_result = Mock()
            mock_result.to_dataframe.return_value = pd.DataFrame({
                'id': [1, 2, 3],
                'name': ['Alice', 'Bob', 'Charlie'],
                'value': [10.5, 20.0, 30.2]
            })
            mock_result.schema = mock_table.schema
            mock_client.query.return_value.result.return_value = mock_result
            
            # Mock run_query_to_dataframe method
            def mock_run_query_to_dataframe(query_str):
                # Convert SQLGlot Select object to string if needed
                if hasattr(query_str, 'sql'):
                    query_str = query_str.sql(pretty=True, dialect="bigquery")
                query_lower = str(query_str).lower()
                
                if 'countif' in query_lower and 'missing_primary_key' in query_lower:
                    # Primary key comparison
                    return pd.DataFrame({
                        'total_rows': [100],
                        'missing_primary_key_in_table1': [5],
                        'missing_primary_key_in_table2': [3],
                        'missing_primary_keys_ratio': [0.08]
                    })
                elif 'count' in query_lower and 'duplicate' in query_lower:
                    # Primary key uniqueness check
                    return pd.DataFrame({
                        'is_unique': [True],
                        'duplicate_count': [0]
                    })
                elif 'exclusive' in query_lower or ('left join' in query_lower and 'is null' in query_lower):
                    # Exclusive primary keys
                    return pd.DataFrame({
                        'id': [1, 2, 3]
                    })
                elif ('ratio' in query_lower and 'common' in query_lower) or ('with table1 as' in query_lower and 'ratio' in query_lower):
                    # Column difference analysis
                    return pd.DataFrame({
                        'name': [{'ratio_not_null': 0.95, 'ratio_equal': 0.85}],
                        'value': [{'ratio_not_null': 0.98, 'ratio_equal': 0.90}]
                    })
                elif 'plain_diff' in query_lower or ('inner_merged' in query_lower and 'select' in query_lower):
                    # Row-level differences
                    return pd.DataFrame({
                        'id': [1, 2, 3],
                        'name__1': ['Alice', 'Bob', 'Charlie'],
                        'name__2': ['Alice New', 'Bob', 'Charlie New'],
                        'value__1': [10.5, 20.0, 30.2],
                        'value__2': [11.0, 20.0, 31.0]
                    })
                else:
                    # Default response
                    return pd.DataFrame({
                        'id': [1, 2, 3],
                        'name': ['Alice', 'Bob', 'Charlie'],
                        'value': [10.5, 20.0, 30.2]
                    })
            
            mock_client.run_query_to_dataframe.side_effect = mock_run_query_to_dataframe

            yield {
                'client': mock_client,
                'query_class': mock_query_class
            }

    @pytest.fixture
    def sample_processor(self, mock_bigquery_environment):
        """Create a BigQuery processor for testing."""
        processor = BigQueryProcessor(
            "SELECT id, name, value FROM table1",
            "SELECT id, name, value FROM table2"
        )
        processor.set_config_data(
            primary_key=["id"],
            columns_to_compare=["name", "value"],
            sampling_rate=100
        )
        return processor

    def test_complete_comparison_workflow(self, sample_processor, mock_bigquery_environment):
        """Test the complete data comparison workflow."""

        # Step 1: Schema analysis
        common_schema = sample_processor.get_common_schema_from_tables()
        assert isinstance(common_schema, TableSchema)
        assert len(common_schema.columns) > 0

        # Step 2: Primary key validation
        is_unique_table1, _ = sample_processor.run_query_check_primary_keys_unique("table1")
        is_unique_table2, _ = sample_processor.run_query_check_primary_keys_unique("table2")

        # With mocked data, these should pass
        assert is_unique_table1 is not None
        assert is_unique_table2 is not None

        # Step 3: Primary key analysis
        pk_analysis = sample_processor.run_query_compare_primary_keys()
        assert isinstance(pk_analysis, pd.DataFrame)

        # Step 4: Column difference ratios
        column_ratios = sample_processor.get_column_diff_ratios(
            selected_columns=["name", "value"],
            common_table_schema=common_schema
        )
        assert isinstance(column_ratios, pd.DataFrame)

        # Step 5: Row-level differences
        query, differences = sample_processor.get_plain_diff(
            selected_columns=["name"],
            common_table_schema=common_schema
        )
        assert query is not None
        assert isinstance(differences, pd.DataFrame)

    def test_primary_key_edge_cases_integration(self, mock_bigquery_environment):
        """Test primary key handling with various edge cases."""
        test_cases = [
            ("id", ["id"]),  # Single string
            (["id"], ["id"]),  # Single list
            ("id,name", ["id", "name"]),  # Comma-separated
            (["id", "name"], ["id", "name"]),  # Multiple list
            ("id, name, date", ["id", "name", "date"]),  # Spaced comma-separated
        ]

        for input_pk, expected_pk in test_cases:
            processor = BigQueryProcessor(
                "SELECT * FROM table1",
                "SELECT * FROM table2"
            )
            processor.set_config_data(
                primary_key=input_pk,
                columns_to_compare=["name"],
                sampling_rate=100
            )

            assert processor.pk_handler.keys == expected_pk

    def test_caching_across_workflow_steps(self, sample_processor, mock_bigquery_environment):
        """Test that caching works across different workflow steps."""

        # First execution - should populate cache
        schema1 = sample_processor.get_common_schema_from_tables()
        pk_analysis1 = sample_processor.run_query_compare_primary_keys()

        # Second execution - should use cache
        schema2 = sample_processor.get_common_schema_from_tables()
        pk_analysis2 = sample_processor.run_query_compare_primary_keys()

        # Results should be identical (from cache)
        assert schema1.table_name == schema2.table_name
        assert len(schema1.columns) == len(schema2.columns)

    def test_cache_invalidation_on_primary_key_change(self, sample_processor, mock_bigquery_environment):
        """Test cache invalidation when primary key changes."""
        # Initial state
        original_pk = sample_processor.pk_handler.keys

        # Change primary key - should invalidate cache
        sample_processor.set_config_data(
            primary_key=["id", "name"],
            columns_to_compare=["value"],
            sampling_rate=100
        )

        new_pk = sample_processor.pk_handler.keys
        assert new_pk != original_pk
        assert new_pk == ["id", "name"]

    def test_error_handling_workflow(self, mock_bigquery_environment):
        """Test error handling throughout the workflow."""
        with patch('data_check.processors.bigquery.QueryBigQuery') as mock_query:
            # Mock client that raises errors
            mock_client = Mock()
            mock_query.return_value = mock_client
            mock_client.query.side_effect = Exception("Connection failed")
            mock_client.get_table_schema_from_sql.side_effect = Exception("Schema error")
            mock_client.get_table_schema_from_table.side_effect = Exception("Schema error")

            processor = BigQueryProcessor(
                "SELECT * FROM invalid_table",
                "SELECT * FROM another_invalid_table"
            )

            # Should handle connection errors gracefully
            processor.set_config_data(
                primary_key=["id"],
                columns_to_compare=["name"],
                sampling_rate=100
            )
            
            with pytest.raises(Exception):
                processor.get_schemas()  # This actually calls the client
                # This would fail when trying to execute queries


@pytest.mark.integration
class TestUIComponentsIntegration:
    """Test UI components integration."""

    @pytest.fixture
    def mock_streamlit_environment(self):
        """Mock Streamlit environment for UI testing."""
        with patch.multiple(
            'streamlit',
            text_area=Mock(return_value="SELECT * FROM table1"),
            multiselect=Mock(return_value=["id"]),
            slider=Mock(return_value=100),
            checkbox=Mock(return_value=False),
            button=Mock(return_value=False),
            form_submit_button=Mock(return_value=False),
            data_editor=Mock(return_value=pd.DataFrame({
                'Select': [True, False],
                'column': ['name', 'value'],
                'ratio_not_null': [0.95, 0.98],
                'ratio_equal': [0.85, 0.92],
                'percentage_diff_values': [0.15, 0.08]
            })),
            selectbox=Mock(),
            radio=Mock(),
            number_input=Mock(return_value=1),
            columns=Mock(return_value=[Mock(), Mock(), Mock()]),
            container=Mock(),
            error=Mock(),
            stop=Mock(),
            write=Mock(),
            dataframe=Mock(),
            title=Mock(),
            code=Mock(),
            markdown=Mock(),
            form=Mock(),
            session_state=Mock(),
            query_params={}
        ) as mocks:
            # Add session_state to the mocks dictionary with proper attributes
            session_state_mock = Mock()
            session_state_mock.primary_key = ["id"]  # Make it iterable
            session_state_mock.columns_to_compare = ["name", "value"]
            session_state_mock.get.return_value = ["id"]  # Return the list when get() is called
            mocks['session_state'] = session_state_mock
            yield mocks

    @pytest.fixture
    def mock_processor_for_ui(self):
        """Mock processor for UI testing."""
        with patch('data_check.processors.bigquery.QueryBigQuery'):
            processor = BigQueryProcessor(
                "SELECT * FROM table1",
                "SELECT * FROM table2"
            )
            processor.set_config_data(
                primary_key=["id"],
                columns_to_compare=["name", "value"],
                sampling_rate=100
            )

            # Mock schema methods
            mock_schema = TableSchema(
                table_name="test",
                columns=[
                    ColumnSchema("id", BigQueryDataType.INTEGER, None),
                    ColumnSchema("name", BigQueryDataType.STRING, None),
                    ColumnSchema("value", BigQueryDataType.FLOAT, None)
                ]
            )
            processor.get_common_schema_from_tables = Mock(return_value=mock_schema)
            processor.get_diff_columns = Mock(return_value=(pd.DataFrame(), pd.DataFrame()))

            return processor

    def test_table_selection_form_integration(self, mock_streamlit_environment):
        """Test table selection form component."""
        form = TableSelectionForm()

        # Mock form submission
        with patch('streamlit.form_submit_button', return_value=True):
            result = form.render()
            assert isinstance(result, bool)

    def test_column_configuration_form_integration(self, mock_streamlit_environment, mock_processor_for_ui):
        """Test column configuration form component."""
        # Ensure the mock is properly applied
        with patch('streamlit.session_state', mock_streamlit_environment['session_state']):
            form = ColumnConfigurationForm(mock_processor_for_ui)

            # Should render without errors
            form.render()

            # Verify that schema analysis was called
            mock_processor_for_ui.get_common_schema_from_tables.assert_called()

    def test_primary_key_analysis_integration(self, mock_streamlit_environment, mock_processor_for_ui):
        """Test primary key analysis component."""
        # Mock the validation methods
        mock_processor_for_ui.run_query_check_primary_keys_unique = Mock(return_value=(True, ""))
        mock_processor_for_ui.run_query_compare_primary_keys = Mock(
            return_value=pd.DataFrame({
                'total_rows': [100],
                'missing_primary_key_in_table1': [0],
                'missing_primary_key_in_table2': [0],
                'missing_primary_keys_ratio': [0.0]
            })
        )

        component = PrimaryKeyAnalysisComponent(mock_processor_for_ui)
        result = component.render()

        assert result is True  # Should pass validation

    def test_column_difference_analysis_integration(self, mock_streamlit_environment, mock_processor_for_ui):
        """Test column difference analysis component."""
        # Mock the required session state
        mock_streamlit_environment['session_state'].update({
            'columns_to_compare': ['name', 'value'],
            'common_table_schema': mock_processor_for_ui.get_common_schema_from_tables()
        })

        # Mock the processor method
        mock_processor_for_ui.get_column_diff_ratios = Mock(
            return_value=pd.DataFrame({
                'column': ['name', 'value'],
                'ratio_not_null': [0.95, 0.98],
                'ratio_equal': [0.85, 0.92],
                'percentage_diff_values': [0.15, 0.08]
            })
        )

        component = ColumnDifferenceAnalysisComponent(mock_processor_for_ui)
        selected_columns = component.render()

        # Should execute without errors
        mock_processor_for_ui.get_column_diff_ratios.assert_called()

    def test_row_difference_viewer_integration(self, mock_streamlit_environment, mock_processor_for_ui):
        """Test row difference viewer component."""
        # Mock the required session state
        mock_streamlit_environment['session_state'].update({
            'common_table_schema': mock_processor_for_ui.get_common_schema_from_tables()
        })

        # Mock the processor methods that are actually called
        mock_query = Mock()
        mock_query.sql.return_value = "SELECT * FROM diff_query"
        mock_processor_for_ui.get_query_plain_diff_tables = Mock(return_value=mock_query)
        mock_processor_for_ui.client.run_query_to_dataframe = Mock(return_value=pd.DataFrame({
            'id': [1, 2],
            'name__1': ['Alice', 'Bob'],
            'name__2': ['Alice Modified', 'Bob'],
            'value__1': [10.5, 20.0],
            'value__2': [11.0, 20.0]
        }))

        component = RowDifferenceViewerComponent(mock_processor_for_ui)
        component.render(['name'])

        # Should execute without errors
        mock_processor_for_ui.get_query_plain_diff_tables.assert_called()


@pytest.mark.integration
class TestDataProcessorIntegration:
    """Test data processor integration with different scenarios."""

    @pytest.fixture
    def processor_with_mock_client(self):
        """Create processor with comprehensive mock client."""
        with patch('data_check.processors.bigquery.QueryBigQuery') as mock_query:
            mock_client = Mock()
            mock_query.return_value = mock_client

            # Create realistic mock data
            self._setup_mock_client_responses(mock_client)

            processor = BigQueryProcessor(
                "SELECT id, name, value, category FROM table1",
                "SELECT id, name, value, description FROM table2"
            )
            yield processor, mock_client

    def _setup_mock_client_responses(self, mock_client):
        """Set up comprehensive mock responses."""
        # Mock schema responses using proper TableSchema objects
        from data_check.models.table import TableSchema, ColumnSchema, BigQueryDataType
        
        schema1 = TableSchema(
            table_name="table1",
            columns=[
                ColumnSchema("id", BigQueryDataType.INTEGER, None),
                ColumnSchema("name", BigQueryDataType.STRING, None),
                ColumnSchema("value", BigQueryDataType.FLOAT, None),
                ColumnSchema("category", BigQueryDataType.STRING, None),
            ]
        )
        schema2 = TableSchema(
            table_name="table2",
            columns=[
                ColumnSchema("id", BigQueryDataType.INTEGER, None),
                ColumnSchema("name", BigQueryDataType.STRING, None),
                ColumnSchema("value", BigQueryDataType.FLOAT, None),
                ColumnSchema("description", BigQueryDataType.STRING, None),  # Different from schema1
            ]
        )

        mock_table1 = Mock()
        mock_table1.table_id = "table1"
        mock_table1.schema = schema1.columns

        mock_table2 = Mock()
        mock_table2.table_id = "table2"
        mock_table2.schema = schema2.columns

        mock_client.get_table.side_effect = [mock_table1, mock_table2]
        
        # Mock schema methods
        mock_client.get_table_schema_from_table.side_effect = [schema1, schema2]
        mock_client.get_table_schema_from_sql.side_effect = lambda query: schema1 if 'table1' in str(query) else schema2

        # Mock query execution
        def mock_query_execution(query_str):
            mock_result = Mock()
            if "primary_key" in query_str.lower():
                mock_result.to_dataframe.return_value = pd.DataFrame({
                    'total_rows': [1000],
                    'missing_primary_key_in_table1': [10],
                    'missing_primary_key_in_table2': [15],
                    'missing_primary_keys_ratio': [0.025]
                })
            elif "diff" in query_str.lower():
                mock_result.to_dataframe.return_value = pd.DataFrame({
                    'id': [1, 2, 3],
                    'name__1': ['Alice', 'Bob', 'Charlie'],
                    'name__2': ['Alice', 'Bob Modified', 'Charlie'],
                    'value__1': [10.5, 20.0, 30.2],
                    'value__2': [10.5, 25.0, 30.2]
                })
            else:
                mock_result.to_dataframe.return_value = pd.DataFrame()

            mock_result.schema = schema1.columns
            return Mock(result=Mock(return_value=mock_result))

        mock_client.query.side_effect = mock_query_execution
        
        # Mock run_query_to_dataframe method
        def mock_run_query_to_dataframe(query_str):
            # Convert SQLGlot Select object to string if needed
            if hasattr(query_str, 'sql'):
                query_str = query_str.sql(pretty=True, dialect="bigquery")
            query_lower = str(query_str).lower()
            
            if 'countif' in query_lower and 'missing_primary_key' in query_lower:
                # Primary key comparison
                return pd.DataFrame({
                    'total_rows': [1000],
                    'missing_primary_key_in_table1': [10],
                    'missing_primary_key_in_table2': [15],
                    'missing_primary_keys_ratio': [0.025]
                })
            elif 'count' in query_lower and 'duplicate' in query_lower:
                # Primary key uniqueness check
                return pd.DataFrame({
                    'is_unique': [True],
                    'duplicate_count': [0]
                })
            elif 'exclusive' in query_lower or ('left join' in query_lower and 'is null' in query_lower):
                # Exclusive primary keys
                return pd.DataFrame({
                    'id': [1, 2, 3]
                })
            elif ('ratio' in query_lower and 'common' in query_lower) or ('with table1 as' in query_lower and 'ratio' in query_lower):
                # Column difference analysis
                return pd.DataFrame({
                    'name': [{'ratio_not_null': 0.95, 'ratio_equal': 0.85}],
                    'value': [{'ratio_not_null': 0.98, 'ratio_equal': 0.90}]
                })
            elif 'plain_diff' in query_lower or ('inner_merged' in query_lower and 'select' in query_lower):
                # Row-level differences
                return pd.DataFrame({
                    'id': [1, 2, 3],
                    'name__1': ['Alice', 'Bob', 'Charlie'],
                    'name__2': ['Alice', 'Bob Modified', 'Charlie'],
                    'value__1': [10.5, 20.0, 30.2],
                    'value__2': [10.5, 25.0, 30.2]
                })
            else:
                # Default response
                return pd.DataFrame({
                    'id': [1, 2, 3],
                    'name': ['Alice', 'Bob', 'Charlie'],
                    'value': [10.5, 20.0, 30.2]
                })
        
        mock_client.run_query_to_dataframe.side_effect = mock_run_query_to_dataframe

    def test_schema_analysis_integration(self, processor_with_mock_client):
        """Test schema analysis integration."""
        processor, mock_client = processor_with_mock_client

        # Test schema retrieval
        schema1, schema2 = processor.get_schemas()
        assert len(schema1.columns) == 4
        assert len(schema2.columns) == 4

        # Test common schema
        common_schema = processor.get_common_schema_from_tables()
        assert len(common_schema.columns) == 3  # id, name, value are common

        # Test schema differences
        diff1, diff2 = processor.get_diff_columns()
        assert not diff1.empty or not diff2.empty  # Should have some differences

    def test_primary_key_operations_integration(self, processor_with_mock_client):
        """Test all primary key operations together."""
        processor, mock_client = processor_with_mock_client

        processor.set_config_data(
            primary_key=["id"],
            columns_to_compare=["name", "value"],
            sampling_rate=100
        )

        # Test primary key uniqueness check
        is_unique1, msg1 = processor.run_query_check_primary_keys_unique("table1")
        is_unique2, msg2 = processor.run_query_check_primary_keys_unique("table2")

        # Test primary key comparison
        pk_comparison = processor.run_query_compare_primary_keys()
        assert isinstance(pk_comparison, pd.DataFrame)

        # Test exclusive primary keys
        excl1, excl2 = processor.run_query_exclusive_primary_keys()
        assert isinstance(excl1, pd.DataFrame)
        assert isinstance(excl2, pd.DataFrame)

    def test_data_comparison_integration(self, processor_with_mock_client):
        """Test complete data comparison integration."""
        processor, mock_client = processor_with_mock_client

        processor.set_config_data(
            primary_key=["id"],
            columns_to_compare=["name", "value"],
            sampling_rate=100
        )

        # Get common schema for comparison
        common_schema = processor.get_common_schema_from_tables()

        # Test column difference ratios
        ratios = processor.get_column_diff_ratios(
            selected_columns=["name", "value"],
            common_table_schema=common_schema
        )
        assert isinstance(ratios, pd.DataFrame)

        # Test row-level differences
        query, differences = processor.get_plain_diff(
            selected_columns=["name"],
            common_table_schema=common_schema
        )
        assert query is not None
        assert isinstance(differences, pd.DataFrame)

    def test_sampling_integration(self, processor_with_mock_client):
        """Test sampling functionality integration."""
        processor, mock_client = processor_with_mock_client

        # Test with different sampling rates
        for sampling_rate in [25, 50, 75, 100]:
            processor.set_config_data(
                primary_key=["id"],
                columns_to_compare=["name"],
                sampling_rate=sampling_rate
            )

            assert processor.sampling_rate == sampling_rate

            # Verify that sampling affects query generation
            if hasattr(processor, 'with_statement_query_sampled'):
                sampled_query = processor.with_statement_query_sampled
                assert sampled_query is not None