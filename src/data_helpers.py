from client import get_columns, query_table, run_query_to_dataframe
from data_processor import compare_tables_primary_key_query, get_query_plain_diff_tables, query_ratio_common_values_per_column
from tools import run_multithreaded
from typing import List, Tuple
import pandas as pd

def get_table_columns(table1:str, table2:str) -> Tuple[List[str], List[str]]:
    jobs = [(get_columns, {"table": table1}), (get_columns, {"table": table2})]
    columns_table_1, columns_table_2 = run_multithreaded(jobs=jobs, max_workers=2)
    return columns_table_1, columns_table_2

def get_dataframes(table1:str, table2:str, columns: list[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    jobs = [(query_table, {'table': table1, 'columns': columns}), (query_table, {'table': table2, 'columns': columns})]
    df1, df2 = run_multithreaded(jobs=jobs, max_workers=2)
    return df1, df2

def get_column_diff_ratios(table1:str, table2:str, primary_key: str, columns: list[str], sampling_rate: int) -> pd.Series:
    query = query_ratio_common_values_per_column(table1=table1, table2=table2, columns=columns, primary_key=primary_key, sampling_rate=sampling_rate)
    df = run_query_to_dataframe(query)
    df = df.transpose().reset_index()
    df.columns = ['column', 'percentage_common_values']
    df['percentage_diff_values'] = 1 - df['percentage_common_values']
    df.sort_values(by='percentage_diff_values', ascending=False, inplace=True)
    return df

def get_plain_diff(table1:str, table2:str, primary_key: str, columns: list[str], sampling_rate: int) -> pd.DataFrame:
    query = get_query_plain_diff_tables(table1, table2, columns, primary_key, sampling_rate)
    df =  run_query_to_dataframe(query)
    return df

def run_query_compare_primary_keys(table1:str, table2:str, primary_key: str) -> pd.DataFrame:
    query = compare_tables_primary_key_query(table1, table2, primary_key)
    return run_query_to_dataframe(query)
