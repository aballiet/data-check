from typing import List, Union

import pandas as pd
import streamlit as st

from data_check.data_formatter import (highlight_diff_dataset, style_gradient,
                                       style_percentage)
from data_check.processors.bigquery import BigQueryProcessor
from data_check.ui.components import render_app_with_components
# Cache manager removed


class DataDiff:
    """Legacy DataDiff class - now using modular components."""

    def __init__(self) -> None:
        st.set_page_config(layout="wide")
        st.title("data-check 🔍")

        # Cache management removed - using direct execution


    def window(self):
        """Main application window using modular components."""
        render_app_with_components()

    # Cache info sidebar removed


if __name__ == "__main__":
    dd = DataDiff()
    dd.window()
