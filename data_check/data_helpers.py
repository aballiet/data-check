from client import get_columns, query_table, run_query_to_dataframe, get_table_schema
from data_formatter import style_percentage
from data_processor import (
    get_query_insight_tables_primary_keys,
    get_query_plain_diff_tables,
    query_ratio_common_values_per_column,
    get_query_exclusive_primary_keys
)
from tools import run_multithreaded
from typing import List, Tuple
from models.table import TableSchema
import pandas as pd


def get_table_columns(table1: str, table2: str) -> Tuple[List[str], List[str]]:
    """Get the columns of two tables"""
    columns_table_1 = get_columns(table=table1)
    columns_table_2 = get_columns(table=table2)
    return columns_table_1, columns_table_2


def get_table_schemas(table1: str, table2: str) -> Tuple[TableSchema, TableSchema]:
    """Get the schemas of two tables"""
    schema_table_1, unsupported_fields1 = get_table_schema(table=table1)
    schema_table_2, unsupported_fields2 = get_table_schema(table=table2)

    if unsupported_fields1 or unsupported_fields2:
        import streamlit as st

        st.warning(
            f"Unsupported RECORD fields: table 1: {unsupported_fields1} / table 2: {unsupported_fields2}, cannot be selected"
        )
    return schema_table_1, schema_table_2


def get_common_schema(table1: str, table2: str) -> TableSchema:
    """Get the common schema of two tables"""
    schema_table_1, schema_table_2 = get_table_schemas(table1=table1, table2=table2)
    common_columns = schema_table_1.get_common_columns(schema_table_2)
    return TableSchema(table_name="common_schema", columns=common_columns)


def get_dataframes(
    table1: str, table2: str, columns: list[str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Get the dataframes of two tables"""
    jobs = [
        (query_table, {"table": table1, "columns": columns}),
        (query_table, {"table": table2, "columns": columns}),
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
    query = query_ratio_common_values_per_column(
        table1=table1,
        table2=table2,
        common_table_schema=filtered_columns,
        primary_key=primary_key,
        sampling_rate=sampling_rate,
    )
    df = run_query_to_dataframe(query)
    df = df.transpose().reset_index()
    df.columns = ["column", "values"]

    df = parse_strucutred_data(df, keys=["ratio_not_null", "ratio_not_equal"])
    df["percentage_diff_values"] = 1 - df["ratio_not_equal"]
    df.sort_values(
        by=["percentage_diff_values", "ratio_not_null"], ascending=False, inplace=True
    )
    return df


def get_plain_diff(
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
    query = get_query_plain_diff_tables(
        table1,
        table2,
        common_table_schema=filtered_columns,
        primary_key=primary_key,
        sampling_rate=sampling_rate,
    )
    df = run_query_to_dataframe(query)
    return df


def run_query_compare_primary_keys(
    table1: str, table2: str, primary_key: str
) -> pd.DataFrame:
    """Compare the primary keys of two tables"""
    query = get_query_insight_tables_primary_keys(table1, table2, primary_key)
    df = run_query_to_dataframe(query)
    return df


def run_query_exclusive_primary_keys(
    table1: str, table2: str, primary_key: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Get the rows where the primary keys are exclusive to one table"""
    df_exclusive_table1 = run_query_to_dataframe(get_query_exclusive_primary_keys(table1, table2, primary_key, exclusive_to="table1"))
    df_exclusive_table1.set_index(primary_key, inplace=True)

    df_exclusive_table2 = run_query_to_dataframe(get_query_exclusive_primary_keys(table1, table2, primary_key, exclusive_to="table2"))
    df_exclusive_table2.set_index(primary_key, inplace=True)
    return df_exclusive_table1, df_exclusive_table2