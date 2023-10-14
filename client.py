import streamlit as st
from google.cloud import bigquery


class BigQueryClient:

    def __init__(self) -> None:
        self.client = bigquery.Client()

    def get_columns(self, table: str) -> list:
        client = bigquery.Client()
        table: bigquery.Table = client.get_table(table)
        return [field.name for field in table.schema]

    # Run query using BigQueryClient and return results in a list of dictionaries
    def run_query(self, query: str) -> list:
        query_job = self.client.query(query)
        return [dict(row) for row in query_job]