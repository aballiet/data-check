import streamlit as st
from google.cloud import bigquery
from typing import Tuple
import pandas as pd
from pandas_gbq import read_gbq
from models.table import TableSchema, ColumnSchema

@st.cache_resource
def init_client():
    return bigquery.Client()

def get_columns(table: str) -> list:
    client = init_client()
    table: bigquery.Table = client.get_table(table)
    return [field.name for field in table.schema]

def get_table_schema(table: str) -> Tuple[TableSchema, list[str]]:
    client = init_client()
    table: bigquery.Table = client.get_table(table)

    columns = []
    unsupported_columns = []
    for field in table.schema:
        # TODO: Support nested structs
        if field.field_type == "RECORD":
            unsupported_columns.append(field.name)
        else:
            columns.append(ColumnSchema(name=field.name, field_type=field.field_type, mode=field.mode))
    return TableSchema(table_name=table.table_id, columns=columns), unsupported_columns

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