from data_check.models.table import TableSchema, ColumnSchema


def test_TableSchema_get_common_columns():
    table1 = TableSchema(
        table_name="table1",
        columns=[
            ColumnSchema(name="col1", field_type="STRING"),
            ColumnSchema(name="col2", field_type="STRING"),
            ColumnSchema(name="col3", field_type="STRING"),
        ],
    )
    table2 = TableSchema(
        table_name="table2",
        columns=[
            ColumnSchema(name="col2", field_type="STRING"),
            ColumnSchema(name="col3", field_type="STRING"),
            ColumnSchema(name="col4", field_type="STRING"),
        ],
    )

    assert table1.get_common_columns(other=table2) == [
        ColumnSchema(name="col2", field_type="STRING"),
        ColumnSchema(name="col3", field_type="STRING"),
    ]
    assert table2.get_common_columns(other=table1) == [
        ColumnSchema(name="col2", field_type="STRING"),
        ColumnSchema(name="col3", field_type="STRING"),
    ]