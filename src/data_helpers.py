from client import get_columns, query_table, run_query_to_dataframe
from data_processor import compare_tables_primary_key_query
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

def run_query_compare_primary_keys(table1:str, table2:str, primary_key: str) -> pd.DataFrame:
    query = compare_tables_primary_key_query(table1, table2, primary_key)
    return run_query_to_dataframe(query)
