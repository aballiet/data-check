from data_check.models.table import ColumnSchema, TableSchema


def test_TableSchema_get_common_columns():
    table1 = TableSchema(
        table_name="table1",
        columns=[
            ColumnSchema(name="col1", field_type="STRING", mode="NULLABLE"),
            ColumnSchema(name="col2", field_type="STRING", mode="NULLABLE"),
            ColumnSchema(name="col3", field_type="STRING", mode="NULLABLE"),
        ],
    )
    table2 = TableSchema(
        table_name="table2",
        columns=[
            ColumnSchema(name="col2", field_type="STRING", mode="NULLABLE"),
            ColumnSchema(name="col3", field_type="STRING", mode="NULLABLE"),
            ColumnSchema(name="col4", field_type="STRING", mode="NULLABLE"),
        ],
    )

    assert set(table1.get_common_column_names(other=table2)) == set(["col2", "col3"])
    assert set(table2.get_common_column_names(other=table1)) == set(["col2", "col3"])
