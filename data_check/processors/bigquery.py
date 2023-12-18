from data_processor import DataProcessor
from models.table import TableSchema
from processors.utils import add_suffix_to_column_names
from query.query_bq import QueryBigQuery
from sqlglot import alias, column, condition, func, parse_one, select
from sqlglot.expressions import Select


class BigQueryProcessor(DataProcessor):
    def __init__(self, query1: str, query2: str) -> None:
        super().__init__(query1, query2, dialect="bigquery", client=QueryBigQuery())

    @property
    def with_statement_query(self) -> Select:
        return (
            select().with_("table1", as_=self.query1).with_("table2", as_=self.query2)
        )

    @property
    def with_statement_query_sampled(self) -> Select:
        if (
            self._table1 is not None
            and self._table2 is not None
            and self.sampling_rate < 100
        ):
            return (
                select()
                .with_(
                    "table1",
                    as_=select("*").from_(
                        f"{self.table1} tablesample system ({self.sampling_rate} percent)",
                        dialect=self.dialect,
                    ),
                )
                .with_(
                    "table2",
                    as_=select("*").from_(
                        f"{self.table2} tablesample system ({self.sampling_rate} percent)",
                        dialect=self.dialect,
                    ),
                )
            )
        return self.with_statement_query

    def check_input_is_sql(self, value: str) -> bool:
        """Check if the input is a SQL query"""
        return " select " in (" " + value).lower() and "from " in value.lower()

    def get_sql_exp_from_tablename(self, tablename: str) -> Select:
        return select("*").from_(tablename, dialect=self.dialect)

    # Create a query to compare two tables common and exlusive primary keys for two tables
    def get_query_insight_tables_primary_keys(self) -> Select:
        """Compare the primary keys of two tables"""

        agg_diff_keys = (
            select(
                alias(func("count", "*"), "total_rows"),
                alias(
                    func("countif", condition(f"table1.{self.primary_key} is null")),
                    "missing_primary_key_in_table1",
                ),
                alias(
                    func("countif", condition(f"table2.{self.primary_key} is null")),
                    "missing_primary_key_in_table2",
                ),
            )
            .from_("table1")
            .join("table2", join_type="full outer", using=self.primary_key)
        )

        query = (
            self.with_statement_query.with_("agg_diff_keys", as_=agg_diff_keys)
            .select(
                "total_rows",
                "missing_primary_key_in_table1",
                "missing_primary_key_in_table2",
                alias(
                    func(
                        "safe_divide",
                        "missing_primary_key_in_table2 + missing_primary_key_in_table1",
                        "total_rows",
                    ),
                    "missing_primary_keys_ratio",
                ),
            )
            .from_("agg_diff_keys")
        )

        return query

    def get_query_exclusive_primary_keys(
        self, exclusive_to: str, limit: int = 500
    ) -> Select:
        common_table_schema = self.get_common_schema_from_tables()

        if exclusive_to == "table1":
            table1_columns_renamed = add_suffix_to_column_names(
                table_name="table1",
                column_names=common_table_schema.columns_names,
                suffix="__1",
            )

            return (
                self.with_statement_query.select(
                    column(self.primary_key, table="table1"), *table1_columns_renamed
                )
                .from_("table1")
                .join("table2", join_type="left", using=self.primary_key)
                .where(f"table2.{self.primary_key} is null")
                .limit(limit)
            )

        if exclusive_to == "table2":
            table1_columns_renamed = add_suffix_to_column_names(
                table_name="table2",
                column_names=common_table_schema.columns_names,
                suffix="__2",
            )

            return (
                self.with_statement_query.select(
                    column(self.primary_key, table="table2"), *table1_columns_renamed
                )
                .from_("table2")
                .join("table1", join_type="left", using=self.primary_key)
                .where(f"table1.{self.primary_key} is null")
                .limit(limit)
            )

    def get_query_plain_diff_tables(
        self,
        common_table_schema: TableSchema,
    ) -> Select:
        """Create a SQL query to get the rows where the columns values are different"""
        cast_fields_1 = common_table_schema.get_query_cast_schema_as_string(
            prefix="", column_name_suffix="__1"
        )
        cast_fields_2 = common_table_schema.get_query_cast_schema_as_string(
            prefix="", column_name_suffix="__2"
        )

        inner_merged = parse_one(
            f"""
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
            from table1
            inner join table2
                using ({self.primary_key})
            """,
            dialect=self.dialect,
        )

        final_result = parse_one(
            f"""
            select *
            from inner_merged
            where {' or '.join([f'coalesce({cast_fields_1[index]}, "none") <> coalesce({cast_fields_2[index]}, "none")' for index in range(len(common_table_schema.columns_names))])}
            """,
            dialect=self.dialect,
        )

        query = (
            self.with_statement_query_sampled.with_(
                "inner_merged", as_=inner_merged, dialect=self.dialect
            )
            .with_("final_result", as_=final_result)
            .select("*")
            .from_("final_result")
        )

        return query

    def query_ratio_common_values_per_column(
        self, common_table_schema: TableSchema
    ) -> Select:
        """Create a SQL query to get the ratio of common values for each column"""

        cast_fields_1 = common_table_schema.get_query_cast_schema_as_string(
            prefix="table1."
        )
        cast_fields_2 = common_table_schema.get_query_cast_schema_as_string(
            prefix="table2."
        )

        count_diff = parse_one(
            f"""
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
            from table1
            inner join table2
                using ({self.primary_key})""",
            dialect=self.dialect,
        )

        final_result = parse_one(
            f"""
            select
            {', '.join(
                [
                    (
                        f"struct("
                            f"safe_divide({col}_count_not_null, count_common) as ratio_not_null"
                            f", safe_divide({col}, {col}_count_not_null) as ratio_equal"
                        f") AS {col}"
                    )
                    for col in common_table_schema.columns_names
                ])
            }
            from count_diff""",
            dialect=self.dialect,
        )

        query = (
            self.with_statement_query_sampled.with_(
                "count_diff", as_=count_diff, dialect=self.dialect
            )
            .with_("final_result", as_=final_result)
            .select("*")
            .from_("final_result")
        )

        return query
