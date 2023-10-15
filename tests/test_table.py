from data_check.models.table import table_schema, column_schema


def test_table_schema_get_common_columns():
    table1 = table_schema(
        table_name="table1",
        columns=[
            column_schema(name="col1", field_type="STRING"),
            column_schema(name="col2", field_type="STRING"),
            column_schema(name="col3", field_type="STRING"),
        ],
    )
    table2 = table_schema(
        table_name="table2",
        columns=[
            column_schema(name="col2", field_type="STRING"),
            column_schema(name="col3", field_type="STRING"),
            column_schema(name="col4", field_type="STRING"),
        ],
    )

    assert table1.get_common_columns(other=table2) == [
        column_schema(name="col2", field_type="STRING"),
        column_schema(name="col3", field_type="STRING"),
    ]
    assert table2.get_common_columns(other=table1) == [
        column_schema(name="col2", field_type="STRING"),
        column_schema(name="col3", field_type="STRING"),
    ]