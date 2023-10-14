import pandas as pd

# Given two DataFrames df1 & df2, compute the ratio of common values for each column, use a given primary_key to compare data
# Example with user_id as primary_key:
# Considering these two dataframe :

#     df1 = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [4, 5, 6, 7], 'C': ['x', 'y', 'z', 'w']})
#     df2 = pd.DataFrame({'A': [1, 2, 3, 5], 'B': [6, 5, 7, 8], 'C': ['z', 'y', 'r', 'v']})

# -> I expect to get : result['B'] = 0.75 and result['C'] = 0.5
def compute_common_value_ratios(df1, df2, primary_key) -> pd.Series:
    # Find common columns
    common_columns = df1.columns.intersection(df2.columns)
    common_columns = common_columns.drop(primary_key)

    if len(common_columns) == 0:
        raise ValueError("No common columns found between the DataFrames.")

    # Initialize a Series to store the results
    result_series = pd.Series(index=common_columns, dtype=float)

    for col in common_columns:
        common_rows = df1.merge(df2, on=[primary_key, col], how='inner')

        if not common_rows.empty:
            common_values = common_rows[col].values
            total_values = df1[col].values
            common_ratio = len(common_values) / len(total_values)
            result_series[col] = common_ratio
        else:
            result_series[col] = 0.0

    return result_series


# Create a query to compare two tables common and exlusive primary keys for two tables
def compare_tables_primary_key_query(table1: str, table2: str, primary_key: str) -> str:
    return f"""
        SELECT COUNT(*) AS total_rows, COUNTIF(table1.{primary_key} != table2.{primary_key}) AS diff_rows
        FROM `{table1}` AS table1
        FULL OUTER JOIN `{table2}` AS table2
        USING ({primary_key})
    """


# Given two tables table1 & table2 and their associated columns columns_table_1 , columns_table_2
# for each common column, output the percentage of differences between the two tables column wise
# Example:
# table1: user_id, name, email
# table2: user_id, name, email
# primary_key: user_id
def percentage_of_differences_per_column(table1: str, columns_table_1: list, table2: str, columns_table_2: list, primary_key: str) -> str:
    columns = set(columns_table_1).intersection(set(columns_table_2))
    columns = list(columns)
    columns.remove(primary_key)
    columns = ", ".join(columns)
    return f"""
        SELECT
            {columns},
            COUNT(*) AS total_rows,
            COUNTIF(table1.{primary_key} != table2.{primary_key}) AS diff_rows,
            ROUND(COUNTIF(table1.{primary_key} != table2.{primary_key}) / COUNT(*) * 100, 2) AS percentage_of_diff
        FROM `{table1}` AS table1
        FULL OUTER JOIN `{table2}` AS table2
        USING ({primary_key})
        GROUP BY {columns}
    """
