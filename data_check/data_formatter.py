import pandas as pd
from pandas.io.formats.style import Styler
from typing import Tuple
import numpy as np
import seaborn as sns


def highlight_selected_text(row: dict):
    text = row["item_name"]
    bold = ["Chocolate"]
    for k in bold:
        text = text.replace(k, f'<span style="background-color:#ddd;">{k}</span>')
    return text


# define function to highlight differences in dataframes
def highlight_diff(data, other, color="yellow"):
    attr = "background-color: {}".format(color)
    return pd.DataFrame(
        np.where(data.ne(other), attr, ""), index=data.index, columns=data.columns
    )


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
