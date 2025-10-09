import pytest
from unittest.mock import patch, Mock
import pandas as pd

from data_check.query.query_bq import QueryBigQuery


@pytest.mark.skip(reason="Requires BigQuery credentials and network access")
def test_bigquery_query_timeout():
    client = QueryBigQuery()
    df = client._run_query_to_dataframe(
        query="SELECT * FROM `bigquery-public-data.samples.shakespeare` LIMIT 1000",
        timeout_seconds=1,
    )

    assert df.shape[0] == 1000
