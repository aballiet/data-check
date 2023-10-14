import pytest
import pandas as pd
from src.data_processor import ComputeDiff

def dummy_dataframe() -> pd.DataFrame:
    return pd.DataFrame({
        'user_id': [1, 2, 3],
        'name': ['John', 'Jane', 'Joe'],
        'last_name': ['Doe', 'Doe', 'Doe'],
    })

def test_compute_common_value_ratios():
    # Example usage
    df1 = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [4, 5, 6, 7], 'C': ['x', 'y', 'z', 'w']})
    df2 = pd.DataFrame({'A': [1, 2, 3, 5], 'B': [6, 5, 7, 8], 'C': ['x', 'y', 'z', 'w']})

    diff = ComputeDiff(
        table1="table1",
        table2="table2",
        df1=df1,
        df2=df2,
        primary_key='A'
    )

    result = diff.compute_common_value_ratios()

    excepted_result = pd.Series(
        [0.75, 0.25, 0.75]
        , index=['A', 'B', 'C']
    )
    pd.testing.assert_series_equal(left=result, right=excepted_result)