import pandas as pd
from data_check.data_processor import get_query_plain_diff_tables, query_ratio_common_values_per_column
from data_check.models.table import TableSchema, ColumnSchema


def test_get_query_plain_diff_tables():

    result = get_query_plain_diff_tables(
        table1="table1",
        table2="table2",
        common_table_schema=TableSchema(table_name="common", columns=[
            ColumnSchema(name='B', field_type='INTEGER', mode='NULLABLE'),
            ColumnSchema(name='C', field_type='STRING', mode='NULLABLE'),
        ]),
        primary_key='A'
    )

    assert result == f"""
    WITH
    inner_merged AS (
        SELECT
            table_1.A
            , table_1.B AS B__1, table_2.B AS B__2, table_1.C AS C__1, table_2.C AS C__2
        FROM `table1` AS table_1
        INNER JOIN `table2` AS table_2
            USING (A)
    )
    SELECT *
    FROM inner_merged
    WHERE cast(B__1 as string) <> cast(B__2 as string) OR C__1 <> C__2
    """

def test_query_ratio_common_values_per_column():
    result = query_ratio_common_values_per_column(
        table1="table1",
        table2="table2",
        common_table_schema=TableSchema(table_name="common", columns=[
            ColumnSchema(name='A', field_type='INTEGER', mode='NULLABLE'),
            ColumnSchema(name='B', field_type='INTEGER', mode='NULLABLE'),
            ColumnSchema(name='C', field_type='STRING', mode='NULLABLE'),
        ]),
        primary_key='A'
    )

    assert result