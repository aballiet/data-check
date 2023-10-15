from client import get_columns, query_table, run_query_to_dataframe, get_table_schema
from data_processor import compare_tables_primary_key_query, get_query_plain_diff_tables, query_ratio_common_values_per_column
from tools import run_multithreaded
from typing import List, Tuple
from models.table import table_schema
import pandas as pd

def get_table_columns(table1:str, table2:str) -> Tuple[List[str], List[str]]:
    """Get the columns of two tables"""
    columns_table_1 = get_columns(table=table1)
    columns_table_2 = get_columns(table=table2)
    return columns_table_1, columns_table_2

def get_table_schemas(table1:str, table2:str) -> Tuple[table_schema, table_schema]:
    """Get the schemas of two tables"""
    schema_table_1 = get_table_schema(table=table1)
    schema_table_2 = get_table_schema(table=table2)
    return schema_table_1, schema_table_2

def get_common_schema(table1:str, table2:str) -> table_schema:
    """Get the common schema of two tables"""
    schema_table_1, schema_table_2 = get_table_schemas(table1=table1, table2=table2)
    common_columns = schema_table_1.get_common_columns(schema_table_2)
    return table_schema(table_name="common_schema", columns=common_columns)

def get_dataframes(table1:str, table2:str, columns: list[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Get the dataframes of two tables"""
    jobs = [(query_table, {'table': table1, 'columns': columns}), (query_table, {'table': table2, 'columns': columns})]
    df1, df2 = run_multithreaded(jobs=jobs, max_workers=2)
    return df1, df2

def get_column_diff_ratios(table1:str, table2:str, primary_key: str, common_schema: table_schema, sampling_rate: int) -> pd.Series:
    """Get the ratio of common values for each column"""
    query = query_ratio_common_values_per_column(table1=table1, table2=table2, common_schema=common_schema, primary_key=primary_key, sampling_rate=sampling_rate)
    df = run_query_to_dataframe(query)
    df = df.transpose().reset_index()
    df.columns = ['column', 'percentage_common_values']
    df['percentage_diff_values'] = 1 - df['percentage_common_values']
    df.sort_values(by='percentage_diff_values', ascending=False, inplace=True)
    return df

def get_plain_diff(table1:str, table2:str, primary_key: str, columns: list[str], sampling_rate: int) -> pd.DataFrame:
    """Get the rows where the columns values are different"""
    query = get_query_plain_diff_tables(table1, table2, columns, primary_key, sampling_rate)
    df =  run_query_to_dataframe(query)
    return df

def run_query_compare_primary_keys(table1:str, table2:str, primary_key: str) -> pd.DataFrame:
    """Compare the primary keys of two tables"""
    query = compare_tables_primary_key_query(table1, table2, primary_key)
    return run_query_to_dataframe(query)
