import pytest
import pandas as pd
from src.data_processor import ComputeDiff, SUFFIX_DATASET_1, SUFFIX_DATASET_2, get_query_plain_diff_tables, query_ratio_common_values_per_column

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

def test_display_diff_rows():
    df1 = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [4, 5, 6, 7], 'C': ['x', 'y', 'z', 'w']})
    df2 = pd.DataFrame({'A': [1, 2, 3, 5], 'B': [6, 5, 7, 8], 'C': ['x', 'y', 'r', 'v']})

    diff = ComputeDiff(
        table1="table1",
        table2="table2",
        df1=df1,
        df2=df2,
        primary_key='A'
    )

    result = diff.display_diff_rows('C')

    # TO FIX : dtypes are now all string
    excepted_result = pd.DataFrame({'A': ['3'], f'B{SUFFIX_DATASET_1}': ['6'], f'C{SUFFIX_DATASET_1}': ['z'], f'B{SUFFIX_DATASET_2}': ['7'], f'C{SUFFIX_DATASET_2}': ['r']}, index=[2])
    pd.testing.assert_frame_equal(left=result, right=excepted_result)


def test_get_query_plain_diff_tables():

    result = get_query_plain_diff_tables(
        table1="table1",
        table2="table2",
        columns=['B', 'C'],
        primary_key='A'
    )

    assert result == f"""
    WITH
    inner_merged AS (
        SELECT
            table_1.A
            , table_1.B AS B__1, table_2.B AS B__2, table_1.C AS C__1, table_2.C AS C__2
        FROM `table1` AS table_1
        INNER JOIN `table2` AS table_2
            USING (A)
    )
    SELECT *
    FROM inner_merged
    WHERE B__1 <> B__2 OR C__1 <> C__2
    """

def test_query_ratio_common_values_per_column():
    result = query_ratio_common_values_per_column(
        table1="table1",
        table2="table2",
        columns=['B', 'C'],
        primary_key='A'
    )

    assert result == f"""
    WITH
    count_diff AS (
        SELECT
            count(A) as count_common
            , countif(table_1.B = table_2.B) AS B, countif(table_1.C = table_2.C) AS C
        FROM `table1` AS table_1
        INNER JOIN `table2` AS table_2
            USING (A)
    )
    SELECT B / count_common AS B, C / count_common AS C
    FROM count_diff
    """