from data_check.processors.bigquery import BigQueryProcessor


def test_bigquery_processor_init():
    query1 = "select * from `my-project.my_dataset.table1`"
    query2 = "select * from `my-project.my_dataset.table2`"

    result = BigQueryProcessor(query1, query2)

    assert result.query1 == query1
    assert result.query2 == query2
    assert result.dialect == "bigquery"
    assert result.client.__class__.__name__ == "QueryBigQuery"


def test_bigquery_processor_init_with_table():
    table1 = "my-project.my_dataset.table1"
    table2 = "my-project.my_dataset.table2"

    result = BigQueryProcessor(table1, table2)

    assert result.query1 == "select * from `my-project.my_dataset.table1`"
    assert result.query2 == "select * from `my-project.my_dataset.table2`"
