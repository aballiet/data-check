from models.table import TableSchema


# Create a query to compare two tables common and exlusive primary keys for two tables
def compare_tables_primary_key_query(table1, table2, primary_key) -> str:
    """Compare the primary keys of two tables"""
    return f"""
        SELECT COUNT(*) AS total_rows, COUNTIF(table1.{primary_key} != table2.{primary_key}) AS diff_rows
        FROM `{table1}` AS table1
        FULL OUTER JOIN `{table2}` AS table2
        USING ({primary_key})
    """

def get_query_plain_diff_tables(table1:str, table2:str, common_table_schema: TableSchema, primary_key: str, sampling_rate: int = 100) -> str:
    """Create a SQL query to get the rows where the columns values are different"""
    cast_fields_1 = common_table_schema.get_query_cast_schema_as_string(prefix="", column_name_suffix="__1")
    cast_fields_2 = common_table_schema.get_query_cast_schema_as_string(prefix="", column_name_suffix="__2")
    query = f"""
    WITH
    inner_merged AS (
        SELECT
            table_1.{primary_key}
            , {', '.join(
                [
                    (
                        f"table_1.{col} AS {col}__1"
                        f", table_2.{col} AS {col}__2"
                    )
                    for col in common_table_schema.columns_names
                ]
            )}
        FROM `{table1}` AS table_1{ f" TABLESAMPLE SYSTEM ({sampling_rate} PERCENT)" if sampling_rate < 100 else "" }
        INNER JOIN `{table2}` AS table_2
            USING ({primary_key})
    )
    SELECT *
    FROM inner_merged
    WHERE {' OR '.join([f'{cast_fields_1[index]} <> {cast_fields_2[index]}' for index in range(len(common_table_schema.columns_names))])}
    """
    print(query)
    return query

def query_ratio_common_values_per_column(table1:str, table2:str, common_table_schema: TableSchema, primary_key: str, sampling_rate: int = 100):
    """Create a SQL query to get the ratio of common values for each column"""

    cast_fields_1 = common_table_schema.get_query_cast_schema_as_string(prefix="table_1.")
    cast_fields_2 = common_table_schema.get_query_cast_schema_as_string(prefix="table_2.")

    query = f"""
    WITH
    count_diff AS (
        SELECT
            count({primary_key}) as count_common
            , {', '.join(
                [
                    (
                        f"countif({cast_fields_1[index]} = {cast_fields_2[index]}) AS {common_table_schema.columns_names[index]}"
                    )
                    for index in range(len(cast_fields_1))
                ]
            )}
        FROM `{table1}` AS table_1{ f" TABLESAMPLE SYSTEM ({sampling_rate} PERCENT)" if sampling_rate < 100 else "" }
        INNER JOIN `{table2}` AS table_2
            USING ({primary_key})
    )
    SELECT {
        ', '.join(
            [
                (
                    f"{col} / count_common AS {col}"
                )
                for col in common_table_schema.columns_names
            ]
        )
    }
    FROM count_diff
    """
    print(query)
    return query