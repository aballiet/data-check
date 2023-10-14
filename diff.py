

# Create a query to compare two tables common and exlusive primary keys for two tables
def compare_tables_primary_key_query(table1: str, table2: str, primary_key: str) -> str:
    return f"""
        SELECT COUNT(*) AS total_rows, COUNTIF(table1.{primary_key} != table2.{primary_key}) AS diff_rows
        FROM `{table1}` AS table1
        FULL OUTER JOIN `{table2}` AS table2
        USING ({primary_key})
    """

# Create a query to compare two tables exlusive columns for two tables, give rows that are different
def compare_exclusive_tables_primary_key_query(table1: str, table2: str, primary_key: str) -> str:
    return f"""
        SELECT table1.*
        FROM `{table1}` AS table1
        LEFT JOIN `{table2}` AS table2
        USING ({primary_key})
        WHERE table2.{primary_key} IS NULL
    """
