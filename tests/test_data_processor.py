import pytest
import pandas as pd
from src.data_processor import compute_common_value_ratios

def dummy_dataframe() -> pd.DataFrame:
    return pd.DataFrame({
        'user_id': [1, 2, 3],
        'name': ['John', 'Jane', 'Joe'],
        'last_name': ['Doe', 'Doe', 'Doe'],
    })

def test_common_rows():
    # Example usage
    df1 = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [4, 5, 6, 7], 'C': ['x', 'y', 'z', 'w']})
    df2 = pd.DataFrame({'A': [1, 2, 3, 5], 'B': [6, 5, 7, 8], 'C': ['z', 'y', 'r', 'v']})

    result = compute_common_value_ratios(df1, df2, primary_key='A')
    assert result['B'] == 0.25
    assert result['C'] == 0.25