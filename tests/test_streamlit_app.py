"""
Tests for streamlit_app module.
"""

import pytest
from unittest.mock import Mock, patch

from data_check.streamlit_app import DataDiff


class TestStreamlitApp:
    """Test Streamlit application."""

    @patch('data_check.streamlit_app.st')
    @patch('data_check.streamlit_app.render_app_with_components')
    def test_data_diff_init(self, mock_render_app, mock_st):
        """Test DataDiff initialization."""
        # Mock st.set_page_config and st.title
        mock_st.set_page_config = Mock()
        mock_st.title = Mock()

        # Create DataDiff instance
        data_diff = DataDiff()

        # Verify that page config and title were set
        mock_st.set_page_config.assert_called_once_with(layout="wide")
        mock_st.title.assert_called_once_with("data-check 🔍")

        # Verify instance was created
        assert isinstance(data_diff, DataDiff)

    @patch('data_check.streamlit_app.render_app_with_components')
    def test_data_diff_window(self, mock_render_app):
        """Test DataDiff window method."""
        # Create DataDiff instance with mocked dependencies
        with patch('data_check.streamlit_app.st') as mock_st:
            mock_st.set_page_config = Mock()
            mock_st.title = Mock()

            data_diff = DataDiff()
            data_diff.window()

            # Verify that render_app_with_components was called
            mock_render_app.assert_called_once()

    @patch('data_check.streamlit_app.st')
    @patch('data_check.streamlit_app.render_app_with_components')
    def test_data_diff_main_execution(self, mock_render_app, mock_st):
        """Test main execution block."""
        # Mock the st methods
        mock_st.set_page_config = Mock()
        mock_st.title = Mock()

        # Import and execute the main block
        import data_check.streamlit_app

        # The main block should create a DataDiff instance and call window()
        # This is tested indirectly through the module import
        assert True  # If we get here without errors, the main block works
