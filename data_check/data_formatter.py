import pandas as pd
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

def convert_float_to_percentage(data: pd.DataFrame, columns):
    """Convert float to percentage for a dataframe"""
    for column in columns:
        data[column] = pd.Series(["{0:.2f}%".format(val * 100) for val in data[column]], index = data.index)
    return data