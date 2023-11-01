import streamlit as st
from google.cloud import bigquery
from typing import Tuple
import pandas as pd
from os import getenv
from pandas_gbq import read_gbq
from models.table import TableSchema, ColumnSchema
from google.oauth2 import service_account

USE_STREAMLIT_SECRET = getenv("USE_STREAMLIT_SECRET", False)

@st.cache_resource
def get_credentials():
    # Create API client from Streamlit Secret
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return credentials

@st.cache_resource
def init_client():
    if not USE_STREAMLIT_SECRET:
        return bigquery.Client()
    credentials = get_credentials()
    return bigquery.Client(credentials=credentials)

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
            columns.append(
                ColumnSchema(
                    name=field.name, field_type=field.field_type, mode=field.mode
                )
            )
    return TableSchema(table_name=table.table_id, columns=columns), unsupported_columns


@st.cache_data(ttl=600)
def run_query_to_dataframe(query: str) -> pd.DataFrame:
    if not USE_STREAMLIT_SECRET:
        return read_gbq(query)

    credentials = get_credentials()
    return read_gbq(query, credentials=credentials)


def query_table(table: str, columns: list[str]) -> pd.DataFrame:
    columns = ", ".join(columns)
    query = f"""
        SELECT
            {columns}
        FROM `{table}`
    """
    return run_query_to_dataframe(query=query)
