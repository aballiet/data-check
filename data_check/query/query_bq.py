from os import getenv
from threading import Thread

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJob
from google.oauth2 import service_account
from sqlglot.expressions import Select

from data_check.models.table import TableSchema
from data_check.query_client import QueryClient

USE_STREAMLIT_SECRET = getenv("USE_STREAMLIT_SECRET", False)
TIMEOUT_BIGQUERY = 900 # 15 * 60 = 15 minutes

class QueryBigQuery(QueryClient):
    def __init__(self):
        self.client = self.init_client()
        self.dialect = "bigquery"

    @st.cache_resource
    def get_credentials(_self):
        # Create API client from Streamlit Secret
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return credentials

    @st.cache_resource
    def init_client(_self) -> bigquery.Client:
        if not USE_STREAMLIT_SECRET:
            return bigquery.Client()
        credentials = _self.get_credentials()
        return bigquery.Client(credentials=credentials)

    def get_table(self, table: str) -> bigquery.Table:
        return self.client.get_table(table)

    def run_query_job_with_timeout(self, query: str, timeout_seconds,):
        # Container for the result
        result = [None]

        query_job = self.client.query(query)

        thread = Thread(target=self.get_query_job_result, args=(query_job, result))
        thread.start()
        thread.join(timeout=timeout_seconds)

        if thread.is_alive():
            self.client.cancel_job(job_id=query_job.job_id)
            raise TimeoutError("BigQuery query took too long to execute, job cancelled.")

        return result[0]

    def _run_query_to_dataframe(_self, query: str, timeout_seconds: int = TIMEOUT_BIGQUERY) -> pd.DataFrame:
        return _self.run_query_job_with_timeout(query, timeout_seconds=timeout_seconds).to_dataframe()

    def run_query_to_dataframe(self, query: Select, timeout_seconds: int = TIMEOUT_BIGQUERY) -> pd.DataFrame:
        return self._run_query_to_dataframe(query.sql(dialect=self.dialect), timeout_seconds=timeout_seconds)

    @staticmethod
    def get_query_job_result(query_job: QueryJob, result):
        result[0] = query_job.result()
        return result

    def run_query_job(_self, query: str) -> bigquery.QueryJob:
        query_job = _self.client.query(query)
        return query_job.result()

    def get_table_schema_from_table(_self, table: str) -> TableSchema:
        """Get the schema of a table"""
        table_bq = _self.client.get_table(table)
        return TableSchema.from_bq_table(table=table_bq)

    def _get_table_schema_from_sql(_self, query: str) -> TableSchema:
        """Get the schema of a table from a query"""
        query_job = _self.run_query_job(query)
        return TableSchema.from_bq_query_job(query_job)

    def get_table_schema_from_sql(self, query: Select) -> TableSchema:
        """Get the schema of a table from a query"""
        query_with_limit = query.limit(50)
        return self._get_table_schema_from_sql(
            query_with_limit.sql(dialect=self.dialect)
        )
