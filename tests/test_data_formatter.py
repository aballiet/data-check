"""
Tests for data_formatter module.
"""

import pytest
import pandas as pd
import numpy as np
from pandas.io.formats.style import Styler

from data_check.data_formatter import (
    style_percentage,
    style_gradient,
    highlight_diff,
    highlight_diff_dataset
)


class TestDataFormatter:
    """Test data formatting functions."""

    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample DataFrame for testing."""
        return pd.DataFrame({
            'col1': [0.1, 0.2, 0.3],
            'col2': [0.4, 0.5, 0.6],
            'col3': [1, 2, 3]
        })

    def test_style_percentage_with_dataframe(self, sample_dataframe):
        """Test style_percentage with DataFrame input."""
        columns = ['col1', 'col2']
        result = style_percentage(sample_dataframe, columns)

        assert isinstance(result, Styler)
        # The function should return a Styler object
        assert result is not None

    def test_style_percentage_with_styler(self, sample_dataframe):
        """Test style_percentage with Styler input."""
        columns = ['col1', 'col2']
        styler = sample_dataframe.style
        result = style_percentage(styler, columns)

        assert isinstance(result, Styler)
        assert result is not None

    def test_style_gradient_with_dataframe(self, sample_dataframe):
        """Test style_gradient with DataFrame input."""
        columns = ['col1', 'col2']
        result = style_gradient(sample_dataframe, columns)

        assert isinstance(result, Styler)
        assert result is not None

    def test_style_gradient_with_styler(self, sample_dataframe):
        """Test style_gradient with Styler input."""
        columns = ['col1', 'col2']
        styler = sample_dataframe.style
        result = style_gradient(styler, columns)

        assert isinstance(result, Styler)
        assert result is not None

    def test_style_gradient_custom_color(self, sample_dataframe):
        """Test style_gradient with custom gradient color."""
        columns = ['col1', 'col2']
        result = style_gradient(sample_dataframe, columns, "blue,green")

        assert isinstance(result, Styler)
        assert result is not None

    def test_highlight_diff(self):
        """Test highlight_diff function."""
        # Create test data with differences
        data = pd.DataFrame({
            'col1__1': ['A', 'B', 'C'],
            'col1__2': ['A', 'X', 'C'],
            'col2__1': [1, 2, 3],
            'col2__2': [1, 2, 4]
        })

        columns = ['col1', 'col2']
        result = highlight_diff(data, columns)

        assert isinstance(result, pd.DataFrame)
        assert result.shape == data.shape
        # Check that differences are highlighted
        assert result.loc[1, 'col1__1'] == 'background-color: #fc9fba'
        assert result.loc[1, 'col1__2'] == 'background-color: #fc9fba'
        assert result.loc[2, 'col2__1'] == 'background-color: #fc9fba'
        assert result.loc[2, 'col2__2'] == 'background-color: #fc9fba'

    def test_highlight_diff_no_differences(self):
        """Test highlight_diff with no differences."""
        data = pd.DataFrame({
            'col1__1': ['A', 'B', 'C'],
            'col1__2': ['A', 'B', 'C'],
            'col2__1': [1, 2, 3],
            'col2__2': [1, 2, 3]
        })

        columns = ['col1', 'col2']
        result = highlight_diff(data, columns)

        assert isinstance(result, pd.DataFrame)
        # All values should be empty strings (no highlighting)
        assert (result == '').all().all()

    def test_highlight_diff_dataset(self):
        """Test highlight_diff_dataset function."""
        data = pd.DataFrame({
            'col1__1': ['A', 'B', 'C'],
            'col1__2': ['A', 'X', 'C'],
            'col2__1': [1, 2, 3],
            'col2__2': [1, 2, 4]
        })

        columns = ['col1', 'col2']
        result = highlight_diff_dataset(data, columns)

        assert isinstance(result, Styler)
        assert result is not None
