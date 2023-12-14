from data_check.data_processor import (
    get_query_plain_diff_tables,
    query_ratio_common_values_per_column,
)
from data_check.models.table import ColumnSchema, TableSchema


def test_get_query_plain_diff_tables():
    result = get_query_plain_diff_tables(
        table1="table1",
        table2="table2",
        common_table_schema=TableSchema(
            table_name="common",
            columns=[
                ColumnSchema(name="B", field_type="INTEGER", mode="NULLABLE"),
                ColumnSchema(name="C", field_type="STRING", mode="NULLABLE"),
            ],
        ),
        primary_key="A",
    )

    assert (
        result
        == f"""
    with
    inner_merged as (
        select
            table_1.A
            , table_1.B as B__1, table_2.B as B__2, table_1.C as C__1, table_2.C as C__2
        from `table1` as table_1
        inner join `table2` as table_2
            using (A)
    )
    select *
    from inner_merged
    where coalesce(cast(B__1 as string), "none") <> coalesce(cast(B__2 as string), "none") or coalesce(C__1, "none") <> coalesce(C__2, "none")
    """
    )


def test_query_ratio_common_values_per_column():
    result = query_ratio_common_values_per_column(
        table1="table1",
        table2="table2",
        common_table_schema=TableSchema(
            table_name="common",
            columns=[
                ColumnSchema(name="A", field_type="INTEGER", mode="NULLABLE"),
                ColumnSchema(name="B", field_type="INTEGER", mode="NULLABLE"),
                ColumnSchema(name="C", field_type="STRING", mode="NULLABLE"),
            ],
        ),
        primary_key="A",
    )

    assert (
        result
        == f"""
    with
    count_diff as (
        select
            count(A) as count_common
            , countif(coalesce(cast(table_1.A as string), cast(table_2.A as string)) is not null) AS A_count_not_null, countif(coalesce(cast(table_1.A as string), 'none') = coalesce(cast(table_2.A as string), 'non')) AS A, countif(coalesce(cast(table_1.B as string), cast(table_2.B as string)) is not null) AS B_count_not_null, countif(coalesce(cast(table_1.B as string), 'none') = coalesce(cast(table_2.B as string), 'non')) AS B, countif(coalesce(table_1.C, table_2.C) is not null) AS C_count_not_null, countif(coalesce(table_1.C, 'none') = coalesce(table_2.C, 'non')) AS C
        from `table1` as table_1
        inner join `table2` as table_2
            using (A)
    )
    select struct(safe_divide(A_count_not_null, count_common) as ratio_not_null, safe_divide(A, A_count_not_null) as ratio_not_equal) AS A, struct(safe_divide(B_count_not_null, count_common) as ratio_not_null, safe_divide(B, B_count_not_null) as ratio_not_equal) AS B, struct(safe_divide(C_count_not_null, count_common) as ratio_not_null, safe_divide(C, C_count_not_null) as ratio_not_equal) AS C
    from count_diff
    """
    )
