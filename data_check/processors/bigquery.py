from data_processor import DataProcessor
from models.table import TableSchema
from query.query_bq import QueryBigQuery
from sqlglot import select, func, alias, condition, column, parse_one
from sqlglot.expressions import Select
from processors.utils import add_suffix_to_column_names


class BigQueryProcessor(DataProcessor):
    def __init__(self, query1: str, query2: str) -> None:
        super().__init__(query1, query2, dialect="bigquery", client=QueryBigQuery())

    @property
    def with_statement(self) -> str:
        return f"""
        with

        table1 as (
            {self.query1}
        ),

        table2 as (
            {self.query2}
        )"""

    @property
    def with_statement_query(self) -> Select:
        return select(
        ).with_(
            "table1", as_=parse_one(self.query1, dialect="bigquery")
        ).with_(
            "table2", as_=parse_one(self.query2, dialect="bigquery"))

    def check_input_is_sql(self, value: str) -> bool:
        """Check if the input is a SQL query"""
        return " select " in (" " + value).lower() and "from " in value.lower()

    def get_sql_exp_from_tablename(self, tablename: str) -> Select:
        return select("*").from_(tablename).sql(dialect=self.dialect)

    # Create a query to compare two tables common and exlusive primary keys for two tables
    def get_query_insight_tables_primary_keys(self) -> str:
        """Compare the primary keys of two tables"""

        agg_diff_keys = select(
            alias(func("count", "*"), "total_rows"),
            alias(func("countif", condition("table1.user_id is null")), "missing_primary_key_in_table1"),
            alias(func("countif", condition("table2.user_id is null")), "missing_primary_key_in_table2")
            ).from_("table1"
            ).join("table2", join_type="full outer", using="user_id"
        )

        query = self.with_statement_query.with_("agg_diff_keys", as_=agg_diff_keys).select(
            "total_rows",
            "missing_primary_key_in_table1",
            "missing_primary_key_in_table2",
            alias (
                func("safe_divide", "missing_primary_key_in_table2 + missing_primary_key_in_table1", "total_rows"),
                "missing_primary_keys_ratio")
            ).from_("agg_diff_keys")

        return query.sql(dialect=self.dialect)

    def get_query_exclusive_primary_keys(
        self, exclusive_to: str, limit: int = 500
    ) -> str:
        common_table_schema = self.get_common_schema_from_tables()

        if exclusive_to == "table1":
            table1_columns_renamed = add_suffix_to_column_names(table_name="table1", column_names=common_table_schema.columns_names, suffix="__1")

            return self.with_statement_query.select(
                column(self.primary_key, table="table1"),
                table1_columns_renamed
            ).from_("table1"
            ).join("table2", join_type="left", using=self.primary_key
            ).where(f"table2.{self.primary_key} is null"
            ).limit(limit).sql(dialect=self.dialect)

        if exclusive_to == "table2":
            table1_columns_renamed = add_suffix_to_column_names(table_name="table2", column_names=common_table_schema.columns_names, suffix="__2")

            return self.with_statement_query.select(
                column(self.primary_key, table="table2"),
                table1_columns_renamed
            ).from_("table2"
            ).join("table1", join_type="left", using=self.primary_key
            ).where(f"table1.{self.primary_key} is null"
            ).limit(limit).sql(dialect=self.dialect)

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
                table1.{self.primary_key}
                , {', '.join(
                    [
                        (
                            f"table1.{col} as {col}__1"
                            f", table2.{col} as {col}__2"
                        )
                        for col in common_table_schema.columns_names
                    ]
                )}
            from table1{ f" tablesample system ({self.sampling_rate} percent)" if self.sampling_rate < 100 else "" }
            inner join table2
                using ({self.primary_key})
        )
        select *
        from inner_merged
        where {' or '.join([f'coalesce({cast_fields_1[index]}, "none") <> coalesce({cast_fields_2[index]}, "none")' for index in range(len(common_table_schema.columns_names))])}
        """
        return query

    def query_ratio_common_values_per_column(self, common_table_schema: TableSchema):
        """Create a SQL query to get the ratio of common values for each column"""

        cast_fields_1 = common_table_schema.get_query_cast_schema_as_string(
            prefix="table1."
        )
        cast_fields_2 = common_table_schema.get_query_cast_schema_as_string(
            prefix="table2."
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
            from table1{ f" tablesample system ({self.sampling_rate} percent)" if self.sampling_rate < 100 else "" }
            inner join table2
                using ({self.primary_key})
        )
        select {
            ', '.join(
                [
                    (
                        f"struct("
                            f"safe_divide({col}_count_not_null, count_common) as ratio_not_null"
                            f", safe_divide({col}, {col}_count_not_null) as ratio_equal"
                        f") AS {col}"
                    )
                    for col in common_table_schema.columns_names
                ]
            )
        }
        from count_diff
        """
        return query
