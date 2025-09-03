from sqlglot import alias, column, condition, func, parse_one, select
from sqlglot.expressions import Select

from data_check.data_processor import DataProcessor
from data_check.models.table import TableSchema
from data_check.query.query_bq import QueryBigQuery

from .utils import add_suffix_to_column_names


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

    def _get_primary_key_concat_expr(self, table_prefix: str) -> str:
        """Get concatenated primary key expression for BigQuery"""
        if len(self.primary_key) == 1:
            return f"{table_prefix}.{self.primary_key[0]}"
        else:
            # For multiple primary keys, concatenate them as strings
            pk_exprs = [f"coalesce(cast({table_prefix}.{pk} as string), '')" for pk in self.primary_key]
            return f"concat({', '.join(pk_exprs)})"

    def _get_primary_key_using_clause(self) -> str:
        """Get USING clause for joins - only works for single primary key"""
        if len(self.primary_key) == 1:
            return self.primary_key[0]
        else:
            # For multiple keys, we can't use USING - need ON condition
            return None

    def _get_primary_key_join_condition(self) -> str:
        """Get join condition for primary keys"""
        if len(self.primary_key) == 1:
            return f"table1.{self.primary_key[0]} = table2.{self.primary_key[0]}"
        else:
            # For multiple primary keys, create a composite key by concatenating
            table1_concat = self._get_primary_key_concat_expr("table1")
            table2_concat = self._get_primary_key_concat_expr("table2")
            return f"{table1_concat} = {table2_concat}"

    # Create a query to compare two tables common and exlusive primary keys for two tables
    def get_query_insight_tables_primary_keys(self) -> Select:
        """Compare the primary keys of two tables"""
        
        table1_pk_expr = self._get_primary_key_concat_expr("table1")
        table2_pk_expr = self._get_primary_key_concat_expr("table2")
        join_condition = self._get_primary_key_join_condition()
        
        # For BigQuery, use concatenated expressions to handle both single and multiple primary keys
        agg_diff_keys = parse_one(f"""
            select
                count(*) as total_rows,
                countif({table1_pk_expr} is null or {table1_pk_expr} = '') as missing_primary_key_in_table1,
                countif({table2_pk_expr} is null or {table2_pk_expr} = '') as missing_primary_key_in_table2
            from table1
            full outer join table2 on {join_condition}
        """, dialect=self.dialect)

        query = (
            self.with_statement_query_sampled.with_("agg_diff_keys", as_=agg_diff_keys)
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

    def get_query_check_primary_keys_unique(self, table_name: str) -> Select:
        """Check if the primary keys are unique for a given row"""
        # Group by all primary keys to check uniqueness
        return (
            self.with_statement_query_sampled.select(
                alias(func("count", "*"), "total_rows"),
            ).from_(table_name, dialect=self.dialect).group_by(*self.primary_key).having(
                func("count", "*") > 1
            )
        )

    def get_query_exclusive_primary_keys(
        self, exclusive_to: str, limit: int = 500
    ) -> Select:
        common_table_schema = self.get_common_schema_from_tables()
        join_condition = self._get_primary_key_join_condition()

        if exclusive_to == "table1":
            table1_columns_renamed = add_suffix_to_column_names(
                table_name="table1",
                column_names=common_table_schema.columns_names,
                suffix="__1",
            )
            pk_select = ', '.join([f'table1.{pk}' for pk in self.primary_key])
            table2_pk_expr = self._get_primary_key_concat_expr("table2")
            
            return parse_one(f"""
                select {pk_select}, {', '.join([col.sql() for col in table1_columns_renamed])}
                from table1
                left join table2 on {join_condition}
                where {table2_pk_expr} is null or {table2_pk_expr} = ''
                limit {limit}
            """, dialect=self.dialect)

        if exclusive_to == "table2":
            table2_columns_renamed = add_suffix_to_column_names(
                table_name="table2",
                column_names=common_table_schema.columns_names,
                suffix="__2",
            )
            pk_select = ', '.join([f'table2.{pk}' for pk in self.primary_key])
            table1_pk_expr = self._get_primary_key_concat_expr("table1")
            
            return parse_one(f"""
                select {pk_select}, {', '.join([col.sql() for col in table2_columns_renamed])}
                from table2
                left join table1 on {join_condition}
                where {table1_pk_expr} is null or {table1_pk_expr} = ''
                limit {limit}
            """, dialect=self.dialect)

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

        # Consistent logic for both single and multiple primary keys
        pk_select = ', '.join([f"table1.{pk}" for pk in self.primary_key])
        join_condition = self._get_primary_key_join_condition()
        
        inner_merged = parse_one(
            f"""
            select
                {pk_select}
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
            inner join table2 on {join_condition}
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

        # Consistent logic for both single and multiple primary keys
        join_condition = self._get_primary_key_join_condition()
        count_expr = f"count(table1.{self.primary_key[0]})"  # Use first primary key for counting with table prefix
        
        count_diff = parse_one(
            f"""
            select
                {count_expr} as count_common
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
            inner join table2 on {join_condition}""",
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
