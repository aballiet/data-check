from data_check.client import get_columns, query_table, run_query_to_dataframe, get_table_schema

def test_get_schema():
    table_schema = get_table_schema(table="gorgias-growth-production.dbt_activation.act_candu_ai_user_traits")
    assert table_schema
