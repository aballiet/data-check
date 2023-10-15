import pandas as pd
from pandas.io.formats.style import Styler
import numpy as np

def highlight_selected_text(row: dict):
    text = row["item_name"]
    bold =['Chocolate']
    for k in bold:
        text = text.replace(k, f'<span style="background-color:#ddd;">{k}</span>')
    return text

# define function to highlight differences in dataframes
def highlight_diff(data, other, color='yellow'):
    attr = 'background-color: {}'.format(color)
    return pd.DataFrame(np.where(data.ne(other), attr, ''),
                        index=data.index, columns=data.columns)

def style_percentage(data: pd.DataFrame, columns) -> Styler:
    return data.style.format("{:.2%}", subset=columns)