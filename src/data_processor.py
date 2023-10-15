import pandas as pd

class ComputeDiff():
    def __init__(self, table1: str, table2: str, df1: pd.DataFrame, df2: pd.DataFrame, primary_key: str) -> None:
        self.table1 = table1
        self.table2 = table2
        self.df1 = df1
        self.df2 = df2
        self.primary_key = primary_key

        assert primary_key in df1.columns, f"Primary key {primary_key} not found in df1."
        assert primary_key in df2.columns, f"Primary key {primary_key} not found in df2."

    # Given two DataFrames df1 & df2, compute the ratio of common values for each column, use a given primary_key to compare data
    # Example with user_id as primary_key:
    # Considering these two dataframe :
    #     df1 = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [4, 5, 6, 7], 'C': ['x', 'y', 'z', 'w']})
    #     df2 = pd.DataFrame({'A': [1, 2, 3, 5], 'B': [6, 5, 7, 8], 'C': ['z', 'y', 'r', 'v']})
    # -> I expect to get : result['B'] = 0.75 and result['C'] = 0.5
    def compute_common_value_ratios(self) -> pd.Series:
        # Find common columns
        common_columns = self.df1.columns.intersection(self.df2.columns)

        if len(common_columns) == 0:
            raise ValueError("No common columns found between the DataFrames.")

        # Initialize a Series to store the results
        result_series = pd.Series(index=common_columns, dtype=float)

        for col in common_columns:
            common_rows = self.df1.astype(str).merge(self.df2.astype(str), on=[self.primary_key, col], how='inner')

            if not common_rows.empty:
                common_values = common_rows[col].values
                total_values = self.df1.astype(str)[col].values
                common_ratio = len(common_values) / len(total_values)
                result_series[col] = common_ratio
            else:
                result_series[col] = 0.0
        return result_series

    # Given two dataframes, a column value and a primary key, display the rows where the column value is different
    def display_diff_rows(self, column: str) -> pd.DataFrame:
        common_rows = self.df1.astype(str).merge(self.df2.astype(str), on=[self.primary_key], how='inner', suffixes=('_x', '_y'))
        diff_rows = common_rows[common_rows[f"{column}_x"] != common_rows[f"{column}_y"]]
        return diff_rows

    def format_common_value_ratios(self) -> pd.DataFrame:
        # Format output
        result_series = self.compute_common_value_ratios()
        df = pd.DataFrame(result_series)
        df.index.name = "column_name"
        df["percentage_of_common_values"] = df[0].apply(lambda x: round(x * 100, 2))
        df["percentage_of_diff"] = 100 - df["percentage_of_common_values"]
        df.drop(columns=[0], inplace=True)
        return df.sort_values(by="percentage_of_diff", ascending=False)


def get_common_columns(columns1: list, columns2: list) -> list:
    common_columns = list(set(columns1).intersection(set(columns2)))
    common_columns.sort()
    return common_columns

# Create a query to compare two tables common and exlusive primary keys for two tables
def compare_tables_primary_key_query(table1, table2, primary_key) -> str:
    return f"""
        SELECT COUNT(*) AS total_rows, COUNTIF(table1.{primary_key} != table2.{primary_key}) AS diff_rows
        FROM `{table1}` AS table1
        FULL OUTER JOIN `{table2}` AS table2
        USING ({primary_key})
    """