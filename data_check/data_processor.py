from models.table import TableSchema


# Create a query to compare two tables common and exlusive primary keys for two tables
def get_query_insight_tables_primary_keys(table1: str, table2: str, primary_key: str) -> str:
    """Compare the primary keys of two tables"""
    query = f"""
        with

        agg_diff_keys as (
            select
                count(*) as total_rows
                , countif(table2.{primary_key} is null) as missing_primary_key_in_table2
                , countif(table1.{primary_key} is null) as missing_primary_key_in_table1
            from `{table1}` as table1
            full outer join `{table2}` as table2
            using ({primary_key})
        )

        select
            total_rows
            , missing_primary_key_in_table2
            , missing_primary_key_in_table1
            , safe_divide(missing_primary_key_in_table2 + missing_primary_key_in_table1, total_rows) as missing_primary_keys_ratio
        from agg_diff_keys
    """
    print(query)
    return query


def get_query_exclusive_primary_keys(table1: str, table2: str, primary_key: str, exclusive_to: str) -> str:
    query = None
    if exclusive_to == "table1":
        query = f"""
        select
            table1.*
        from `{table1}` as table1
        left join `{table2}` as table2 using ({primary_key})
        where table2.{primary_key} is null
        """

    else:
        query = f"""
        select
            table2.*
        from `{table2}` as table2
        left join `{table1}` as table1 using ({primary_key})
        where table1.{primary_key} is null
        """
    print(query)
    return query


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
