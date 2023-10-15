from client import get_columns, query_table, run_query_to_dataframe
from data_processor import compare_tables_primary_key_query, get_query_plain_diff_tables
from tools import run_multithreaded
from typing import List, Tuple
import pandas as pd

def get_table_columns(table1:str, table2:str) -> Tuple[List[str], List[str]]:
    jobs = [(get_columns, {"table": table1}), (get_columns, {"table": table2})]
    columns_table_1, columns_table_2 = run_multithreaded(jobs=jobs, max_workers=2)
    return columns_table_1, columns_table_2

def get_dataframes(table1:str, table2:str, primary_key: str, columns: list[str], sampling_rate: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    query = get_query_plain_diff_tables(table1, table2, columns, primary_key, sampling_rate)
    df =  run_query_to_dataframe(query)

    df1 = df[[col for col in df.columns if col.endswith('__1')]]
    df2 = df[[col for col in df.columns if col.endswith('__2')]]

    df1.rename(columns={f"{primary_key}__1": primary_key}, inplace=True)
    df2.rename(columns={f"{primary_key}__2": primary_key}, inplace=True)

    return df1, df2

def run_query_compare_primary_keys(table1:str, table2:str, primary_key: str) -> pd.DataFrame:
    query = compare_tables_primary_key_query(table1, table2, primary_key)
    return run_query_to_dataframe(query)
