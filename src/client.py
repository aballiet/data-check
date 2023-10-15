import streamlit as st
from google.cloud import bigquery
import pandas as pd
from pandas_gbq import read_gbq
from models.table import table_schema, column_schema

@st.cache_resource
def init_client():
    return bigquery.Client()

def get_columns(table: str) -> list:
    client = init_client()
    table: bigquery.Table = client.get_table(table)
    return [field.name for field in table.schema]

def get_table_schema(table: str) -> table_schema:
    client = init_client()
    table: bigquery.Table = client.get_table(table)
    columns = [column_schema(name=field.name, field_type=field.field_type) for field in table.schema]
    return table_schema(table_name=table.table_id, columns=columns)

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