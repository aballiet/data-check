import streamlit as st
from google.cloud import bigquery
import pandas as pd


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

    def run_query_to_dataframe(self, query: str) -> pd.DataFrame:
        query_job = self.client.query(query)
        return query_job.to_dataframe()

    def get_common_columns(self, table1:str, table2:str) -> list:
        columns_table_1 = self.get_columns(table1)
        columns_table_2 = self.get_columns(table2)
        common_columns = set(columns_table_1).intersection(set(columns_table_2))
        return list(common_columns)

    def query_table(self, table:str , columns: list[str]) -> pd.DataFrame:
        columns = ", ".join(columns)
        query = f"""
            SELECT
                {columns}
            FROM `{table}`
        """
        return self.run_query_to_dataframe(query=query)