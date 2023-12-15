from data_check.processors.utils import add_suffix_to_column_names

def test_utils_add_suffix_to_column_names():
    result = add_suffix_to_column_names(
        table_name="table1",
        column_names=["A", "B", "C"],
        suffix="__1",
    )

    assert result[0].sql() == 'table1.A AS A__1'
    assert result[1].sql() == 'table1.B AS B__1'