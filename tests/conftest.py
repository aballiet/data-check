"""Comprehensive test fixtures and mocks for data_check testing."""

import pandas as pd
import pytest
from unittest.mock import MagicMock, Mock, patch
from typing import List, Dict, Any
from google.cloud import bigquery

from data_check.models.table import TableSchema, ColumnSchema, BigQueryDataType, BigQueryDataMode


@pytest.fixture
def mock_bigquery_client():
    """Mock BigQuery client for testing database operations."""
    with patch('google.cloud.bigquery.Client') as mock_client:
        client_instance = Mock()
        mock_client.return_value = client_instance

        # Mock table schema
        mock_table = Mock(spec=bigquery.Table)
        mock_table.table_id = "test_table"
        mock_table.schema = [
            Mock(name="id", field_type="INTEGER", mode=None),
            Mock(name="name", field_type="STRING", mode=None),
            Mock(name="value", field_type="FLOAT", mode=None),
            Mock(name="tags", field_type="STRING", mode="REPEATED"),
        ]

        client_instance.get_table.return_value = mock_table

        # Mock query results
        mock_result = Mock()
        mock_result.to_dataframe.return_value = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10.5, 20.0, 30.2]
        })
        mock_result.schema = mock_table.schema

        client_instance.query.return_value.result.return_value = mock_result

        yield client_instance


@pytest.fixture
def sample_table_schemas():
    """Sample table schemas for testing."""
    schema1 = TableSchema(
        table_name="table1",
        columns=[
            ColumnSchema("id", BigQueryDataType.INTEGER, None),
            ColumnSchema("name", BigQueryDataType.STRING, None),
            ColumnSchema("value", BigQueryDataType.FLOAT, None),
            ColumnSchema("tags", BigQueryDataType.STRING, BigQueryDataMode.REPEATED),
        ]
    )

    schema2 = TableSchema(
        table_name="table2",
        columns=[
            ColumnSchema("id", BigQueryDataType.INTEGER, None),
            ColumnSchema("name", BigQueryDataType.STRING, None),
            ColumnSchema("value", BigQueryDataType.FLOAT, None),
            ColumnSchema("description", BigQueryDataType.STRING, None),
        ]
    )

    return schema1, schema2


@pytest.fixture
def mock_streamlit_session_state():
    """Mock Streamlit session state for UI testing."""
    with patch('streamlit.session_state') as mock_state:
        mock_state.config_tables = False
        mock_state.loaded_tables = False
        mock_state.table1 = "SELECT * FROM test.table1"
        mock_state.table2 = "SELECT * FROM test.table2"
        mock_state.primary_key = ["id"]
        mock_state.columns_to_compare = ["name", "value"]
        mock_state.sampling_rate = 100
        mock_state.is_select_all = False
        mock_state.common_table_schema = None
        yield mock_state


@pytest.fixture
def performance_timer():
    """Timer fixture for performance testing."""
    import time

    class Timer:
        def __init__(self):
            self.start_time = None
            self.end_time = None

        def start(self):
            self.start_time = time.perf_counter()
            return self

        def stop(self):
            self.end_time = time.perf_counter()
            return self

        @property
        def duration(self):
            if self.start_time is None or self.end_time is None:
                raise ValueError("Timer not properly started/stopped")
            return self.end_time - self.start_time

    return Timer()


@pytest.fixture
def sample_dataframes():
    """Sample dataframes for testing data processing."""
    df1 = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'value': [10.5, 20.0, 30.2, 40.1],
        'tags': [['tag1'], ['tag2', 'tag3'], ['tag1', 'tag3'], ['tag4']]
    })

    df2 = pd.DataFrame({
        'id': [1, 2, 3, 5],
        'name': ['Alice', 'Bob Modified', 'Charlie', 'Eve'],
        'value': [10.5, 25.0, 30.2, 50.0],
        'description': ['Desc1', 'Desc2', 'Desc3', 'Desc5']
    })

    return df1, df2


@pytest.fixture
def test_datasets():
    """Test datasets for comprehensive testing scenarios."""
    return {
        'small': pd.DataFrame({
            'id': range(10),
            'value': [i * 1.5 for i in range(10)]
        }),
        'medium': pd.DataFrame({
            'id': range(1000),
            'value': [i * 1.5 for i in range(1000)]
        }),
        'large': pd.DataFrame({
            'id': range(10000),
            'value': [i * 1.5 for i in range(10000)]
        })
    }


@pytest.fixture
def mock_query_results():
    """Mock query results for different scenarios."""
    return {
        'primary_key_analysis': pd.DataFrame({
            'total_rows': [1000],
            'missing_primary_key_in_table1': [10],
            'missing_primary_key_in_table2': [15],
            'missing_primary_keys_ratio': [0.025]
        }),
        'column_diff_ratios': pd.DataFrame({
            'column': ['name', 'value', 'description'],
            'ratio_not_null': [0.95, 0.98, 0.80],
            'ratio_equal': [0.85, 0.92, 0.75],
            'percentage_diff_values': [0.15, 0.08, 0.25]
        }),
        'row_differences': pd.DataFrame({
            'id': [1, 2, 3],
            'name__1': ['Alice', 'Bob', 'Charlie'],
            'name__2': ['Alice', 'Bob Modified', 'Charlie'],
            'value__1': [10.5, 20.0, 30.2],
            'value__2': [10.5, 25.0, 30.2]
        })
    }


@pytest.fixture
def primary_key_test_cases():
    """Test cases for primary key scenarios."""
    return {
        'single_key': ['id'],
        'multiple_keys': ['id', 'date'],
        'complex_keys': ['customer_id', 'product_id', 'transaction_date'],
        'edge_cases': {
            'empty': [],
            'none': None,
            'single_as_string': 'id'
        }
    }


@pytest.fixture
def mock_streamlit_components():
    """Mock Streamlit components for UI testing."""
    with patch.multiple(
        'streamlit',
        write=Mock(),
        dataframe=Mock(),
        text_area=Mock(),
        multiselect=Mock(return_value=['id']),
        slider=Mock(return_value=100),
        checkbox=Mock(return_value=False),
        button=Mock(return_value=False),
        form_submit_button=Mock(return_value=False),
        data_editor=Mock(),
        selectbox=Mock(),
        radio=Mock(),
        number_input=Mock(return_value=1),
        columns=Mock(return_value=[Mock(), Mock(), Mock()]),
        container=Mock(),
        error=Mock(),
        stop=Mock(),
        title=Mock(),
        code=Mock(),
        markdown=Mock(),
        form=Mock(),
        cache_data=lambda func: func,
        cache_resource=lambda func: func,
        set_page_config=Mock(),
        query_params={}
    ) as mocks:
        yield mocks


@pytest.fixture
def sql_query_examples():
    """Sample SQL queries for testing."""
    return {
        'simple_select': "SELECT * FROM table1",
        'complex_select': """
            SELECT
                id,
                name,
                value,
                CASE WHEN value > 10 THEN 'high' ELSE 'low' END as category
            FROM table1
            WHERE date >= '2023-01-01'
        """,
        'with_joins': """
            SELECT t1.id, t1.name, t2.description
            FROM table1 t1
            LEFT JOIN table2 t2 ON t1.id = t2.id
        """,
        'invalid_sql': "INVALID SQL QUERY"
    }


@pytest.fixture(autouse=True)
def reset_caches():
    """Automatically reset caches between tests."""
    import streamlit as st
    # Clear any existing caches
    if hasattr(st, 'cache_data'):
        st.cache_data.clear()
    if hasattr(st, 'cache_resource'):
        st.cache_resource.clear()
    yield
    # Clean up after test
    if hasattr(st, 'cache_data'):
        st.cache_data.clear()
    if hasattr(st, 'cache_resource'):
        st.cache_resource.clear()


class MockBigQueryJob:
    """Mock BigQuery job for testing."""
    def __init__(self, result_data: pd.DataFrame = None):
        self.job_id = "test_job_123"
        self.schema = [
            Mock(name="id", field_type="INTEGER", mode=None),
            Mock(name="name", field_type="STRING", mode=None),
        ]
        self._result_data = result_data or pd.DataFrame()

    def result(self):
        mock_result = Mock()
        mock_result.to_dataframe.return_value = self._result_data
        mock_result.schema = self.schema
        return mock_result


@pytest.fixture
def error_scenarios():
    """Error scenarios for testing error handling."""
    return {
        'connection_error': Exception("Connection failed"),
        'timeout_error': TimeoutError("Query timeout"),
        'invalid_table': Exception("Table not found"),
        'invalid_sql': Exception("SQL syntax error"),
        'permission_error': Exception("Permission denied")
    }