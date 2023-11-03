import streamlit as st
from google.cloud import bigquery
import pandas as pd
from os import getenv
from pandas_gbq import read_gbq
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

@st.cache_data(ttl=600)
def get_table(table: str) -> bigquery.Table:
    client = init_client()
    return client.get_table(table)

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
