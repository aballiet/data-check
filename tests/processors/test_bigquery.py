import pytest

from data_check.models.table import ColumnSchema, TableSchema
from data_check.processors.bigquery import BigQueryProcessor

QUERY_1 = "select * from `my-project.my_dataset.table1`"
QUERY_2 = "select * from `my-project.my_dataset.table2`"

# Create fixture for BigQueryProcessor
@pytest.fixture
def bigquery_processor() -> BigQueryProcessor:
    processor = BigQueryProcessor(QUERY_1, QUERY_2)
    processor.set_config_data(
        primary_key="A",
        columns_to_compare=["B", "C"],
        sampling_rate=100,
    )
    return processor

def test_bigquery_processor_init(bigquery_processor: BigQueryProcessor):
    assert bigquery_processor.query1.sql() == 'SELECT * FROM "my-project"."my_dataset"."table1"'
    assert bigquery_processor.query2.sql() == 'SELECT * FROM "my-project"."my_dataset"."table2"'
    assert bigquery_processor.dialect == "bigquery"
    assert bigquery_processor.client.__class__.__name__ == "QueryBigQuery"


def test_bigquery_processor_init_with_table():
    table1 = "my-project.my_dataset.table1"
    table2 = "my-project.my_dataset.table2"

    result = BigQueryProcessor(table1, table2)

    assert result.query1.sql() == 'SELECT * FROM my-project.my_dataset.table1'
    assert result.query2.sql() == 'SELECT * FROM my-project.my_dataset.table2'

def test_get_query_plain_diff_tables():

    processor = BigQueryProcessor("table1", "table2")
    processor.set_config_data(
        primary_key="A",
        columns_to_compare=["B", "C"],
        sampling_rate=100,
    )

    result = processor.get_query_plain_diff_tables(
        common_table_schema=TableSchema(
            table_name="common",
            columns=[
                ColumnSchema(name="B", field_type="INTEGER", mode="NULLABLE"),
                ColumnSchema(name="C", field_type="STRING", mode="NULLABLE"),
            ],
        )
    )

    assert (
        result.sql()
        == f"""WITH table1 AS (SELECT * FROM table1), table2 AS (SELECT * FROM table2), inner_merged AS (SELECT table1.A, table1.B AS B__1, table2.B AS B__2, table1.C AS C__1, table2.C AS C__2 FROM table1 INNER JOIN table2 ON table1.A = table2.A), final_result AS (SELECT * FROM inner_merged WHERE COALESCE(CAST(B__1 AS TEXT), "none") <> COALESCE(CAST(B__2 AS TEXT), "none") OR COALESCE(C__1, "none") <> COALESCE(C__2, "none")) SELECT * FROM final_result"""
    )


def test_query_ratio_common_values_per_column():

    processor = BigQueryProcessor("table1", "table2")
    processor.set_config_data(
        primary_key="A",
        columns_to_compare=["B", "C"],
        sampling_rate=100,
    )

    result = processor.query_ratio_common_values_per_column(
        common_table_schema=TableSchema(
            table_name="common",
            columns=[
                ColumnSchema(name="A", field_type="INTEGER", mode="NULLABLE"),
                ColumnSchema(name="B", field_type="INTEGER", mode="NULLABLE"),
                ColumnSchema(name="C", field_type="STRING", mode="NULLABLE"),
            ],
        )
    )

    assert (
        result.sql()
        == f"WITH table1 AS (SELECT * FROM table1), table2 AS (SELECT * FROM table2), count_diff AS (SELECT COUNT(table1.A) AS count_common, COUNT_IF(NOT COALESCE(CAST(table1.A AS TEXT), CAST(table2.A AS TEXT)) IS NULL) AS A_count_not_null, COUNT_IF(COALESCE(CAST(table1.A AS TEXT), 'none') = COALESCE(CAST(table2.A AS TEXT), 'non')) AS A, COUNT_IF(NOT COALESCE(CAST(table1.B AS TEXT), CAST(table2.B AS TEXT)) IS NULL) AS B_count_not_null, COUNT_IF(COALESCE(CAST(table1.B AS TEXT), 'none') = COALESCE(CAST(table2.B AS TEXT), 'non')) AS B, COUNT_IF(NOT COALESCE(table1.C, table2.C) IS NULL) AS C_count_not_null, COUNT_IF(COALESCE(table1.C, 'none') = COALESCE(table2.C, 'non')) AS C FROM table1 INNER JOIN table2 ON table1.A = table2.A), final_result AS (SELECT STRUCT(CASE WHEN count_common <> 0 THEN A_count_not_null / count_common ELSE NULL END AS ratio_not_null, CASE WHEN A_count_not_null <> 0 THEN A / A_count_not_null ELSE NULL END AS ratio_equal) AS A, STRUCT(CASE WHEN count_common <> 0 THEN B_count_not_null / count_common ELSE NULL END AS ratio_not_null, CASE WHEN B_count_not_null <> 0 THEN B / B_count_not_null ELSE NULL END AS ratio_equal) AS B, STRUCT(CASE WHEN count_common <> 0 THEN C_count_not_null / count_common ELSE NULL END AS ratio_not_null, CASE WHEN C_count_not_null <> 0 THEN C / C_count_not_null ELSE NULL END AS ratio_equal) AS C FROM count_diff) SELECT * FROM final_result"
    )


def test_get_query_check_primary_keys_unique(bigquery_processor: BigQueryProcessor):
    query1 = bigquery_processor.get_query_check_primary_keys_unique(table_name="table1")
    assert query1.sql() == 'WITH table1 AS (SELECT * FROM "my-project"."my_dataset"."table1"), table2 AS (SELECT * FROM "my-project"."my_dataset"."table2") SELECT COUNT(*) AS total_rows FROM table1 GROUP BY A HAVING COUNT(*) > 1'


def test_multiple_primary_keys():
    """Test BigQuery processor with multiple primary keys"""
    processor = BigQueryProcessor("table1", "table2")
    processor.set_config_data(
        primary_key=["A", "B"],  # Multiple primary keys
        columns_to_compare=["C"],
        sampling_rate=100,
    )

    # Test the primary key properties
    assert processor.primary_key == ["A", "B"]
    
    # Test primary key concatenation expression
    table1_expr = processor._get_primary_key_concat_expr("table1")
    expected_table1_expr = "concat(coalesce(cast(table1.A as string), ''), coalesce(cast(table1.B as string), ''))"
    assert table1_expr == expected_table1_expr
    
    # Test join condition
    join_condition = processor._get_primary_key_join_condition()
    expected_join = "concat(coalesce(cast(table1.A as string), ''), coalesce(cast(table1.B as string), '')) = concat(coalesce(cast(table2.A as string), ''), coalesce(cast(table2.B as string), ''))"
    assert join_condition == expected_join


@pytest.mark.skip(reason="Need BigQuery credentials to run this test")
def test_run_query_check_primary_keys_unique():
    processor = BigQueryProcessor("MY_TABLE", "MY_TABLE_2")
    processor.set_config_data(
        primary_key="user_id",
        columns_to_compare=["user_id", "account_automation"],
        sampling_rate=100,
    )

    result = processor.run_query_check_primary_keys_unique(table="table1")
    assert result == (True, "")
