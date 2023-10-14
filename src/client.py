import streamlit as st
from google.cloud import bigquery
import pandas as pd
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