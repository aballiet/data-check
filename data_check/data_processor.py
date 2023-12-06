from models.table import TableSchema
from abc import ABC, abstractmethod
from typing import List, Tuple
import pandas as pd
from tools import run_multithreaded

class DataProcessor(ABC):

    def __init__(self, query1: str, query2: str, use_sql: bool, sampling_rate: int) -> None:
        self.query1 = query1
        self.query2 = query2
        self.sampling_rate = sampling_rate
        self.use_sql = use_sql
        self._primary_key = None

    @property.setter
    def primary_key(self) -> str:
        return "id"

    ###### ABSTRACT METHODS ######
    @abstractmethod
    def get_credentials(self):
        pass

    @abstractmethod
    def init_client(self):
        pass

    @abstractmethod
    def get_table(self, table: str): # TODO : remove bigquery.Table
        pass

    @abstractmethod
    def run_query_to_dataframe(self, query: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def query_table(self, table: str, columns: list[str]) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_query_insight_tables_primary_keys(self, table1: str, table2: str, primary_key: str) -> str:
        """Compare the primary keys of two tables"""
        pass

    @abstractmethod
    def get_query_exclusive_primary_keys(self, table1: str, table2: str, primary_key: str, exclusive_to: str) -> str:
        pass

    @abstractmethod
    def get_query_plain_diff_tables(
        self,
        table1: str,
        table2: str,
        common_table_schema: TableSchema,
        primary_key: str,
        sampling_rate: int = 100,
    ) -> str:
        """Create a SQL query to get the rows where the columns values are different"""
        pass

    @abstractmethod
    def query_ratio_common_values_per_column(
        self,
        table1: str,
        table2: str,
        common_table_schema: TableSchema,
        primary_key: str,
        sampling_rate: int = 100,
    ):
        """Create a SQL query to get the ratio of common values for each column"""
        pass

    @abstractmethod
    def get_tables_schemas(self, table1: str, table2: str) -> Tuple[TableSchema, TableSchema]:
        """Get the schema of a table"""
        pass

    ###### METHODS ######
    def get_table_columns(self, table1: str, table2: str) -> Tuple[List[str], List[str]]:
        """Get the columns of two tables"""
        schema_table_1, schema_table_2 = self.get_tables_schemas(table1, table2)
        return schema_table_1.columns_names, schema_table_2.columns_names

    def get_table_schemas_warning(self, table1: str, table2: str) -> Tuple[TableSchema, TableSchema]:
        """Get the schemas of two tables"""
        schema_table_1, schema_table_2 = self.get_tables_schemas(table1, table2)

        if schema_table_1.get_unsupported_fields() or schema_table_2.get_unsupported_fields():
            import streamlit as st

            st.warning(
                f"Unsupported RECORD fields: table 1: {schema_table_1.get_unsupported_fields()} / table 2: {schema_table_2.get_unsupported_fields()}, cannot be selected"
            )
        return schema_table_1, schema_table_2


    def get_diff_columns(self, schema_table_1: TableSchema, schema_table_2: TableSchema) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Returns a mapping of columns that are different per table"""
        common_columns_names = schema_table_1.get_common_column_names(schema_table_2, include_unsupported=True)
        diff_columns_table_1 = [
            column for column in schema_table_1.columns if column.name not in common_columns_names or column.field_type != schema_table_2.get_column(column.name).field_type
        ]
        diff_columns_table_2 = [
            column for column in schema_table_2.columns if column.name not in common_columns_names or column.field_type != schema_table_1.get_column(column.name).field_type
        ]
        diff_1_table_schema = TableSchema(table_name="diff_1_table", columns=diff_columns_table_1)
        diff_2_table_schema = TableSchema(table_name="diff_2_table", columns=diff_columns_table_2)
        return diff_1_table_schema.to_dataframe(), diff_2_table_schema.to_dataframe()


    def get_common_schema(self, table1: str, table2: str) -> TableSchema:
        """Get the common schema of two tables"""
        schema_table_1, schema_table_2 = self.get_table_schemas_warning(table1=table1, table2=table2)
        common_columns = schema_table_1.get_common_column_names(schema_table_2, include_unsupported=False)
        return TableSchema(table_name="common_schema", columns=common_columns)


    def get_dataframes(
        self, table1: str, table2: str, columns: list[str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Get the dataframes of two tables"""
        jobs = [
            (self.query_table, {"table": table1, "columns": columns}),
            (self.query_table, {"table": table2, "columns": columns}),
        ]
        df1, df2 = run_multithreaded(jobs=jobs, max_workers=2)
        return df1, df2


    def parse_strucutred_data(
        data: pd.DataFrame, keys: str, column: str = "values"
    ) -> pd.DataFrame:
        """Parse the structured data"""
        data = data.copy()
        for key in keys:
            data[key] = data[column].apply(lambda x: x[key])
        data.drop(columns=[column], inplace=True)
        return data


    def get_column_diff_ratios(
        self,
        table1: str,
        table2: str,
        primary_key: str,
        selected_columns: List[str],
        common_table_schema: TableSchema,
        sampling_rate: int,
    ) -> pd.DataFrame:
        """Get the ratio of common values for each column"""
        filtered_columns = TableSchema(
            table_name="filtered_columns",
            columns=[common_table_schema.get_column(column) for column in selected_columns],
        )
        query = self.query_ratio_common_values_per_column(
            table1=table1,
            table2=table2,
            common_table_schema=filtered_columns,
            primary_key=primary_key,
            sampling_rate=sampling_rate,
        )
        df = self.run_query_to_dataframe(query)
        df = df.transpose().reset_index()
        df.columns = ["column", "values"]

        df = self.parse_strucutred_data(df, keys=["ratio_not_null", "ratio_not_equal"])
        df["percentage_diff_values"] = 1 - df["ratio_not_equal"]
        df.sort_values(
            by=["percentage_diff_values", "ratio_not_null"], ascending=False, inplace=True
        )
        return df


    def get_plain_diff(
        self,
        table1: str,
        table2: str,
        primary_key: str,
        selected_columns: List[str],
        common_table_schema: TableSchema,
        sampling_rate: int,
    ) -> pd.DataFrame:
        """Get the rows where the columns values are different"""
        filtered_columns = TableSchema(
            table_name="filtered_columns",
            columns=[common_table_schema.get_column(column) for column in selected_columns],
        )
        query = self.get_query_plain_diff_tables(
            table1,
            table2,
            common_table_schema=filtered_columns,
            primary_key=primary_key,
            sampling_rate=sampling_rate,
        )
        df = self.run_query_to_dataframe(query)
        return df


    def run_query_compare_primary_keys(
        self, table1: str, table2: str, primary_key: str
    ) -> pd.DataFrame:
        """Compare the primary keys of two tables"""
        query = self.get_query_insight_tables_primary_keys(table1, table2, primary_key)
        df = self.run_query_to_dataframe(query)
        return df


    def run_query_exclusive_primary_keys(
        self, table1: str, table2: str, primary_key: str,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Get the rows where the primary keys are exclusive to one table"""
        df_exclusive_table1 = self.run_query_to_dataframe(self.get_query_exclusive_primary_keys(table1, table2, primary_key, exclusive_to="table1"))
        df_exclusive_table1.set_index(primary_key, inplace=True)

        df_exclusive_table2 = self.run_query_to_dataframe(self.get_query_exclusive_primary_keys(table1, table2, primary_key, exclusive_to="table2"))
        df_exclusive_table2.set_index(primary_key, inplace=True)
        return df_exclusive_table1, df_exclusive_table2
