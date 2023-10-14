import streamlit as st
from google.cloud import bigquery
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pandas_gbq import read_gbq

@st.cache_resource
def init_client():
    return bigquery.Client()

def get_columns(table: str) -> list:
    client = init_client()
    table: bigquery.Table = client.get_table(table)
    return [field.name for field in table.schema]

@st.cache_data
def run_query_to_dataframe(query: str) -> pd.DataFrame:
    return read_gbq(query)

def query_table(table:str , columns: list[str]) -> pd.DataFrame:
    columns = ", ".join(columns)
    query = f"""
        SELECT
            {columns}
        FROM `{table}`
    """
    return run_query_to_dataframe(query=query)

def get_common_columns(table1:str, table2:str) -> list:
    with ThreadPoolExecutor(max_workers=2) as executor:  # Adjust max_workers as needed
        future1 = executor.submit(get_columns, table1)
        future2 = executor.submit(get_columns, table2)

    columns_table_1 = future1.result()
    columns_table_2 = future2.result()

    common_columns = list(set(columns_table_1).intersection(set(columns_table_2)))
    common_columns.sort()
    return common_columns

def get_dataframes(table1: str, table2: str, common_columns: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    with ThreadPoolExecutor(max_workers=2) as executor:  # Adjust max_workers as needed
        future1 = executor.submit(query_table, table1, common_columns)
        future2 = executor.submit(query_table, table2, common_columns)

    df1 = future1.result()
    df2 = future2.result()
    return df1, df2