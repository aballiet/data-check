import pytest

from src.diff import percentage_of_differences_per_column


# create SQL query to create two tables with columns ["user_id", "name", "email"] and 3 rows with half common values
@pytest.fixture
def create_tables():
    return """
        CREATE TABLE `table1` AS
        SELECT 1 AS user_id, "John" AS name, "toto@gmail.com" AS email UNION ALL
        SELECT 2 AS user_id, "Jane" AS name, "toto4@gmail.com" AS email UNION ALL
        SELECT 3 AS user_id, "Jack" AS name, "      " AS email;
        """


def test_percentage_per_column():
    table1 = "table1"
    columns_table_1 = ["user_id", "name", "email"]
    table2 = "table2"
    columns_table_2 = ["user_id", "name", "email"]
    primary_key = "user_id"
    result = percentage_of_differences_per_column(table1, columns_table_1, table2, columns_table_2, primary_key)
    expected = """
        SELECT
            email, name,
            COUNT(*) AS total_rows,
            COUNTIF(table1.user_id != table2.user_id) AS diff_rows,
            ROUND(COUNTIF(table1.user_id != table2.user_id) / COUNT(*) * 100, 2) AS percentage_of_diff
        FROM `table1` AS table1
        FULL OUTER JOIN `table2` AS table2
        USING (user_id)
        GROUP BY email, name
    """
    assert result == expected