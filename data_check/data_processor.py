from abc import ABC, abstractmethod
from typing import List, Tuple

import pandas as pd
from sqlglot import parse_one
from sqlglot.expressions import Select

from .models.table import TableSchema
from .query_client import QueryClient


class DataProcessor(ABC):
    def __init__(
        self,
        query1: str,
        query2: str,
        dialect: str,
        client: QueryClient,
    ) -> None:
        self.client = client
        self.dialect = dialect

        self.use_sql_query1 = False
        self.use_sql_query2 = False

        if self.check_input_is_sql(query1):
            self.use_sql_query1 = True
            self.query1 = parse_one(query1.strip().strip("\r\n"), dialect=self.dialect)
            self._table1 = None
        else:
            self._table1 = query1.strip().strip("\r\n")
            self.query1 = self.get_sql_exp_from_tablename(self._table1)

        if self.check_input_is_sql(query2):
            self.use_sql_query2 = True
            self.query2 = parse_one(query2.strip().strip("\r\n"), dialect=self.dialect)
            self._table2 = None
        else:
            self._table2 = query2.strip().strip("\r\n")
            self.query2 = self.get_sql_exp_from_tablename(self._table2)

        # Needed for full diff
        self._primary_key = None
        self._columns_to_compare = None
        self._sampling_rate = None

    def set_config_data(
        self, primary_key: str, columns_to_compare: List[str], sampling_rate: int
    ):
        self._primary_key = primary_key
        self._columns_to_compare = columns_to_compare
        self._sampling_rate = sampling_rate

    @property
    def primary_key(self) -> str:
        if self._primary_key is None:
            raise ValueError("primary_key is not set")
        return self._primary_key

    @property
    def columns_to_compare(self) -> List[str]:
        if self._columns_to_compare is None:
            raise ValueError("columns_to_compare is not set")
        return self._columns_to_compare

    @property
    def table1(self) -> str:
        if self._table1 is None:
            raise ValueError("table1 is not set")
        return self._table1

    @property
    def table2(self) -> str:
        if self._table2 is None:
            raise ValueError("table2 is not set")
        return self._table2

    @property
    def sampling_rate(self) -> int:
        if self._sampling_rate is None:
            raise ValueError("sampling_rate is not set")
        return self._sampling_rate

    @property
    def is_sampling_allowed(self) -> bool:
        """Returns True if sampling is allowed"""
        return (not self.use_sql_query1) and (not self.use_sql_query2)

    @abstractmethod
    def check_input_is_sql(self, value: str) -> bool:
        """Check if the input is a SQL query"""
        pass

    @abstractmethod
    def get_sql_exp_from_tablename(self, tablename: str) -> Select:
        """Get the SQL query from a table name"""
        pass

    @abstractmethod
    def get_query_insight_tables_primary_keys(self) -> Select:
        """Compare the primary keys of two tables"""
        pass

    @abstractmethod
    def get_query_check_primary_keys_unique(self, table_name: str) -> Select:
        """Check if the primary keys are unique for a given row"""
        pass

    @abstractmethod
    def get_query_exclusive_primary_keys(self, exclusive_to: str) -> Select:
        pass

    @abstractmethod
    def get_query_plain_diff_tables(
        self,
        common_table_schema: TableSchema,
    ) -> Select:
        """Create a SQL query to get the rows where the columns values are different"""
        pass

    @abstractmethod
    def query_ratio_common_values_per_column(
        self,
        common_table_schema: TableSchema,
    ) -> Select:
        """Create a SQL query to get the ratio of common values for each column"""
        pass

    ###### METHODS ######
    def get_schemas(self) -> Tuple[TableSchema, TableSchema]:
        if self.use_sql_query1:
            schema_table_1 = self.client.get_table_schema_from_sql(self.query1)
        else:
            schema_table_1 = self.client.get_table_schema_from_table(self.table1)

        if self.use_sql_query2:
            schema_table_2 = self.client.get_table_schema_from_sql(self.query2)
        else:
            schema_table_2 = self.client.get_table_schema_from_table(self.table2)

        return schema_table_1, schema_table_2

    def get_table_columns(self) -> Tuple[List[str], List[str]]:
        """Get the columns of two tables"""
        schema_table_1, schema_table_2 = self.get_schemas()
        return schema_table_1.columns_names, schema_table_2.columns_names

    def get_schemas_with_warning(self) -> Tuple[TableSchema, TableSchema]:
        """Get the schemas of two tables"""
        schema_table_1, schema_table_2 = self.get_schemas()

        if (
            schema_table_1.get_unsupported_fields()
            or schema_table_2.get_unsupported_fields()
        ):
            import streamlit as st

            st.warning(
                f"Unsupported RECORD fields: table 1: {schema_table_1.get_unsupported_fields()} / table 2: {schema_table_2.get_unsupported_fields()}, cannot be selected"
            )
        return schema_table_1, schema_table_2

    def get_diff_columns(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Returns a mapping of columns that are different per table"""
        schema_table_1, schema_table_2 = self.get_schemas()

        common_columns_names = schema_table_1.get_common_column_names(
            schema_table_2, include_unsupported=True
        )
        diff_columns_table_1 = [
            column
            for column in schema_table_1.columns
            if column.name not in common_columns_names
            or column.field_type != schema_table_2.get_column(column.name).field_type
        ]
        diff_columns_table_2 = [
            column
            for column in schema_table_2.columns
            if column.name not in common_columns_names
            or column.field_type != schema_table_1.get_column(column.name).field_type
        ]
        diff_1_table_schema = TableSchema(
            table_name="diff_1_table", columns=diff_columns_table_1
        )
        diff_2_table_schema = TableSchema(
            table_name="diff_2_table", columns=diff_columns_table_2
        )
        return diff_1_table_schema.to_dataframe(), diff_2_table_schema.to_dataframe()

    def get_common_schema_from_tables(self) -> TableSchema:
        """Get the common schema of two tables"""
        schema_table_1, schema_table_2 = self.get_schemas_with_warning()
        common_columns = schema_table_1.get_common_column_names(
            schema_table_2, include_unsupported=False
        )
        common_columns = [
            schema_table_1.get_column(column) for column in common_columns
        ]
        return TableSchema(table_name="common_schema", columns=common_columns)

    def parse_strucutred_data(
        self, data: pd.DataFrame, keys: List[str], column: str = "values"
    ) -> pd.DataFrame:
        """Parse the structured data"""
        data = data.copy()
        for key in keys:
            data[key] = data[column].apply(lambda x: x[key])
        data.drop(columns=[column], inplace=True)
        return data

    def get_column_diff_ratios(
        self,
        selected_columns: List[str],
        common_table_schema: TableSchema,
    ) -> pd.DataFrame:
        """Get the ratio of common values for each column"""
        filtered_columns = TableSchema(
            table_name="filtered_columns",
            columns=[
                common_table_schema.get_column(column) for column in selected_columns
            ],
        )
        query = self.query_ratio_common_values_per_column(
            common_table_schema=filtered_columns
        )
        df = self.client.run_query_to_dataframe(query)
        df = df.transpose().reset_index()
        df.columns = ["column", "values"]

        df = self.parse_strucutred_data(df, keys=["ratio_not_null", "ratio_equal"])
        df["percentage_diff_values"] = 1 - df["ratio_equal"]
        df.sort_values(
            by=["percentage_diff_values", "ratio_not_null"],
            ascending=False,
            inplace=True,
        )
        return df

    def get_plain_diff(
        self,
        selected_columns: List[str],
        common_table_schema: TableSchema,
    ) -> Tuple[Select, pd.DataFrame]:
        """Get the rows where the columns values are different"""
        filtered_columns = TableSchema(
            table_name="filtered_columns",
            columns=[
                common_table_schema.get_column(column) for column in selected_columns
            ],
        )
        query = self.get_query_plain_diff_tables(
            common_table_schema=filtered_columns,
        )
        df = self.client.run_query_to_dataframe(query)
        return query, df
    
    def run_query_check_primary_keys_unique(self, table: str) -> Tuple[bool, str]:
        """Check if the primary keys are unique for a given row"""
        query = self.get_query_check_primary_keys_unique(table_name=table)
        df = self.client.run_query_to_dataframe(query)

        if not df.empty:
            error_message = f"Primary key is not unique for {table}: . You can use the query: {query.sql()} to check it."
            return False, error_message

        return True, ""

    def run_query_compare_primary_keys(self) -> pd.DataFrame:
        """Compare the primary keys of two tables"""
        query = self.get_query_insight_tables_primary_keys()
        df = self.client.run_query_to_dataframe(query)
        return df

    def run_query_exclusive_primary_keys(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Get the rows where the primary keys are exclusive to one table"""
        df_exclusive_table1 = self.client.run_query_to_dataframe(
            self.get_query_exclusive_primary_keys(exclusive_to="table1")
        )
        df_exclusive_table1.set_index(self.primary_key, inplace=True)

        df_exclusive_table2 = self.client.run_query_to_dataframe(
            self.get_query_exclusive_primary_keys(exclusive_to="table2")
        )
        df_exclusive_table2.set_index(self.primary_key, inplace=True)
        return df_exclusive_table1, df_exclusive_table2
