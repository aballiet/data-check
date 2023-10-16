from models.table import TableSchema


# Create a query to compare two tables common and exlusive primary keys for two tables
def compare_tables_primary_key_query(table1, table2, primary_key) -> str:
    """Compare the primary keys of two tables"""
    return f"""
        select count(*) as total_rows, countif(table1.{primary_key} != table2.{primary_key}) as diff_rows
        from `{table1}` as table1
        full outer join `{table2}` as table2
        using ({primary_key})
    """


def get_query_plain_diff_tables(
    table1: str,
    table2: str,
    common_table_schema: TableSchema,
    primary_key: str,
    sampling_rate: int = 100,
) -> str:
    """Create a SQL query to get the rows where the columns values are different"""
    cast_fields_1 = common_table_schema.get_query_cast_schema_as_string(
        prefix="", column_name_suffix="__1"
    )
    cast_fields_2 = common_table_schema.get_query_cast_schema_as_string(
        prefix="", column_name_suffix="__2"
    )
    query = f"""
    with
    inner_merged as (
        select
            table_1.{primary_key}
            , {', '.join(
                [
                    (
                        f"table_1.{col} as {col}__1"
                        f", table_2.{col} as {col}__2"
                    )
                    for col in common_table_schema.columns_names
                ]
            )}
        from `{table1}` as table_1{ f" tablesample system ({sampling_rate} percent)" if sampling_rate < 100 else "" }
        inner join `{table2}` as table_2
            using ({primary_key})
    )
    select *
    from inner_merged
    where {' or '.join([f'{cast_fields_1[index]} <> {cast_fields_2[index]}' for index in range(len(common_table_schema.columns_names))])}
    """
    print(query)
    return query


def query_ratio_common_values_per_column(
    table1: str,
    table2: str,
    common_table_schema: TableSchema,
    primary_key: str,
    sampling_rate: int = 100,
):
    """Create a SQL query to get the ratio of common values for each column"""

    cast_fields_1 = common_table_schema.get_query_cast_schema_as_string(
        prefix="table_1."
    )
    cast_fields_2 = common_table_schema.get_query_cast_schema_as_string(
        prefix="table_2."
    )

    query = f"""
    with
    count_diff as (
        select
            count({primary_key}) as count_common
            , {', '.join(
                [
                    (
                        f"countif(coalesce({cast_fields_1[index]}, {cast_fields_2[index]}) is not null) AS {common_table_schema.columns_names[index]}_count_not_null"
                        f", countif({cast_fields_1[index]} = {cast_fields_2[index]}) AS {common_table_schema.columns_names[index]}"
                    )
                    for index in range(len(cast_fields_1))
                ]
            )}
        from `{table1}` as table_1{ f" tablesample system ({sampling_rate} percent)" if sampling_rate < 100 else "" }
        inner join `{table2}` as table_2
            using ({primary_key})
    )
    select {
        ', '.join(
            [
                (
                    f"struct("
                        f"safe_divide({col}_count_not_null, count_common) as ratio_not_null"
                        f", safe_divide({col}, {col}_count_not_null) as ratio_not_equal"
                    f") AS {col}"
                )
                for col in common_table_schema.columns_names
            ]
        )
    }
    from count_diff
    """
    print(query)
    return query
