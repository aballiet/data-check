from sqlglot import alias, column, condition, func, select
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
        
        # Always use ON condition for consistency
        join_condition_expr = condition(self._get_primary_key_join_condition())
        agg_diff_keys = (
            select(
                alias(func("count", "*"), "total_rows"),
                alias(
                    func("countif", condition(f"{table1_pk_expr} is null or {table1_pk_expr} = ''")),
                    "missing_primary_key_in_table1",
                ),
                alias(
                    func("countif", condition(f"{table2_pk_expr} is null or {table2_pk_expr} = ''")),
                    "missing_primary_key_in_table2",
                ),
            )
            .from_("table1")
            .join("table2", join_type="full outer", on=join_condition_expr)
        )

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

        if exclusive_to == "table1":
            table1_columns_renamed = add_suffix_to_column_names(
                table_name="table1",
                column_names=common_table_schema.columns_names,
                suffix="__1",
            )
            pk_columns = [column(pk, table="table1") for pk in self.primary_key]
            join_condition_expr = condition(self._get_primary_key_join_condition())
            table2_pk_expr = self._get_primary_key_concat_expr("table2")
            
            return (
                self.with_statement_query_sampled
                .select(*pk_columns, *table1_columns_renamed)
                .from_("table1")
                .join("table2", join_type="left", on=join_condition_expr)
                .where(f"{table2_pk_expr} is null or {table2_pk_expr} = ''")
                .limit(limit)
            )

        if exclusive_to == "table2":
            table2_columns_renamed = add_suffix_to_column_names(
                table_name="table2",
                column_names=common_table_schema.columns_names,
                suffix="__2",
            )
            pk_columns = [column(pk, table="table2") for pk in self.primary_key]
            join_condition_expr = condition(self._get_primary_key_join_condition())
            table1_pk_expr = self._get_primary_key_concat_expr("table1")
            
            return (
                self.with_statement_query_sampled
                .select(*pk_columns, *table2_columns_renamed)
                .from_("table2")
                .join("table1", join_type="left", on=join_condition_expr)
                .where(f"{table1_pk_expr} is null or {table1_pk_expr} = ''")
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

        # Consistent logic for both single and multiple primary keys  
        pk_columns = [column(pk, table="table1") for pk in self.primary_key]
        data_columns = []
        for col in common_table_schema.columns_names:
            data_columns.extend([
                alias(column(col, table="table1"), f"{col}__1"),
                alias(column(col, table="table2"), f"{col}__2")
            ])
        
        # Always use ON condition for consistency
        join_condition = condition(self._get_primary_key_join_condition())
        inner_merged = (
            select(*pk_columns, *data_columns)
            .from_("table1") 
            .join("table2", join_type="inner", on=join_condition)
        )

        # Build the final result query with WHERE conditions for differences
        where_conditions = []
        for index in range(len(common_table_schema.columns_names)):
            where_conditions.append(
                condition(f'coalesce({cast_fields_1[index]}, "none") <> coalesce({cast_fields_2[index]}, "none")')
            )
        
        final_result = (
            select("*")
            .from_("inner_merged")
            .where(func("or", *where_conditions) if len(where_conditions) > 1 else where_conditions[0])
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
        # Build count_diff query using sqlglot expressions
        count_columns = [alias(func("count", column(self.primary_key[0], table="table1")), "count_common")]
        
        for index, col_name in enumerate(common_table_schema.columns_names):
            count_columns.extend([
                alias(
                    func("countif", condition(f"coalesce({cast_fields_1[index]}, {cast_fields_2[index]}) is not null")),
                    f"{col_name}_count_not_null"
                ),
                alias(
                    func("countif", condition(f"coalesce({cast_fields_1[index]}, 'none') = coalesce({cast_fields_2[index]}, 'non')")),
                    col_name
                )
            ])
        
        # Always use ON condition for consistency
        join_condition_expr = condition(self._get_primary_key_join_condition())
        count_diff = (
            select(*count_columns)
            .from_("table1")
            .join("table2", join_type="inner", on=join_condition_expr)
        )

        # Build final_result query using sqlglot expressions
        struct_columns = []
        for col_name in common_table_schema.columns_names:
            struct_columns.append(
                alias(
                    func("struct", 
                        alias(func("safe_divide", column(f"{col_name}_count_not_null"), column("count_common")), "ratio_not_null"),
                        alias(func("safe_divide", column(col_name), column(f"{col_name}_count_not_null")), "ratio_equal")
                    ),
                    col_name
                )
            )
        
        final_result = select(*struct_columns).from_("count_diff")

        query = (
            self.with_statement_query_sampled.with_(
                "count_diff", as_=count_diff, dialect=self.dialect
            )
            .with_("final_result", as_=final_result)
            .select("*")
            .from_("final_result")
        )

        return query
