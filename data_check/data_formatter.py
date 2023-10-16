import pandas as pd
import streamlit as st
from pandas.io.formats.style import Styler
from typing import Tuple, List
import numpy as np
import seaborn as sns


def highlight_selected_text(row: dict):
    text = row["item_name"]
    bold = ["Chocolate"]
    for k in bold:
        text = text.replace(k, f'<span style="background-color:#ddd;">{k}</span>')
    return text

def style_percentage(data: Tuple[pd.DataFrame, Styler], columns) -> Styler:
    if isinstance(data, pd.DataFrame):
        return data.style.format("{:.2%}", subset=columns)
    elif isinstance(data, Styler):
        return data.format("{:.2%}", subset=columns)


def style_gradient(data: Tuple[pd.DataFrame, Styler], columns) -> Styler:
    cmap = sns.color_palette("blend:white,red", as_cmap=True)
    if isinstance(data, pd.DataFrame):
        return data.style.background_gradient(cmap=cmap, subset=columns)
    elif isinstance(data, Styler):
        return data.background_gradient(cmap=cmap, subset=columns)


# Define a custom styling function
def highlight_diff(row, columns: List[str]):
    styles = [''] * len(row)
    for column in columns:
        if row[f"{column}__1"] != row[f"{column}__2"]:
            styles = ['background-color: #ffcccc'] * len(row)
    return styles

def highlight_diff_dataset(data: pd.DataFrame, columns: str) -> Styler:
    # Apply the styling function
    styled_df = data.style.apply(highlight_diff, columns=columns, axis=1)
    return styled_df