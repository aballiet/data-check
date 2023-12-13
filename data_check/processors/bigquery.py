from query.query_bq import QueryBigQuery
from data_processor import DataProcessor
from models.table import TableSchema

class BigQueryProcessor(DataProcessor):

    def __init__(self, query1: str, query2: str, use_sql: bool, sampling_rate: int) -> None:
        super().__init__(query1, query2, use_sql, sampling_rate, QueryBigQuery())

    @property
    def with_statement(self) -> str:
        return f"""
            with
            table1 as ({self.query1}),
            table2 as ({self.query2})
        """

    # Create a query to compare two tables common and exlusive primary keys for two tables
    def get_query_insight_tables_primary_keys(self) -> str:
        """Compare the primary keys of two tables"""
        query = f"""
            {self.with_statement},

            agg_diff_keys as (
                select
                    count(*) as total_rows
                    , countif(table1.{self.primary_key} is null) as missing_primary_key_in_table1
                    , countif(table2.{self.primary_key} is null) as missing_primary_key_in_table2
                from table1
                full outer join table2
                using ({self.primary_key})
            )

            select
                total_rows
                , missing_primary_key_in_table1
                , missing_primary_key_in_table2
                , safe_divide(missing_primary_key_in_table2 + missing_primary_key_in_table1, total_rows) as missing_primary_keys_ratio
            from agg_diff_keys
        """
        return query

    def get_query_exclusive_primary_keys(self, exclusive_to: str) -> str:
        query = None
        if exclusive_to == "table1":
            query = f"""
            {self.with_statement}

            select
                table1.*
            from table1
            left join table2 using ({self.primary_key})
            where table2.{self.primary_key} is null
            """

        else:
            query = f"""
            {self.with_statement}
            select
                table2.*
            from table2
            left join table1 using ({self.primary_key})
            where table1.{self.primary_key} is null
            """
        return query

    def get_query_plain_diff_tables(
    self,
    common_table_schema: TableSchema,
) -> str:
        """Create a SQL query to get the rows where the columns values are different"""
        cast_fields_1 = common_table_schema.get_query_cast_schema_as_string(
            prefix="", column_name_suffix="__1"
        )
        cast_fields_2 = common_table_schema.get_query_cast_schema_as_string(
            prefix="", column_name_suffix="__2"
        )
        query = f"""
        {self.with_statement},

        inner_merged as (
            select
                table_1.{self.primary_key}
                , {', '.join(
                    [
                        (
                            f"table_1.{col} as {col}__1"
                            f", table_2.{col} as {col}__2"
                        )
                        for col in common_table_schema.columns_names
                    ]
                )}
            from table_1{ f" tablesample system ({self.sampling_rate} percent)" if self.sampling_rate < 100 else "" }
            inner join table_2
                using ({self.primary_key})
        )
        select *
        from inner_merged
        where {' or '.join([f'coalesce({cast_fields_1[index]}, "none") <> coalesce({cast_fields_2[index]}, "none")' for index in range(len(common_table_schema.columns_names))])}
        """
        return query

    def query_ratio_common_values_per_column(
        self,
        common_table_schema: TableSchema,
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
        {self.with_statement},

        count_diff as (
            select
                count({self.primary_key}) as count_common
                , {', '.join(
                    [
                        (
                            f"countif(coalesce({cast_fields_1[index]}, {cast_fields_2[index]}) is not null) AS {common_table_schema.columns_names[index]}_count_not_null"
                            f", countif(coalesce({cast_fields_1[index]}, 'none') = coalesce({cast_fields_2[index]}, 'non')) AS {common_table_schema.columns_names[index]}"
                        )
                        for index in range(len(cast_fields_1))
                    ]
                )}
            from table_1{ f" tablesample system ({sampling_rate} percent)" if sampling_rate < 100 else "" }
            inner join table_2
                using ({self.primary_key})
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
        return query
