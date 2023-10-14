

# Create a query to compare two tables common and exlusive primary keys for two tables
def compare_tables_primary_key_query(table1: str, table2: str, primary_key: str) -> str:
    return f"""
        SELECT COUNT(*) AS total_rows, COUNTIF(table1.{primary_key} != table2.{primary_key}) AS diff_rows
        FROM `{table1}` AS table1
        FULL OUTER JOIN `{table2}` AS table2
        USING ({primary_key})
    """










# Given two tables table1 & table2 and their associated columns columns_table_1 , columns_table_2
# for each common column, output the percentage of differences between the two tables column wise
# Example:
# table1: user_id, name, email
# table2: user_id, name, email
# primary_key: user_id
def percentage_of_differences_per_column(table1: str, columns_table_1: list, table2: str, columns_table_2: list, primary_key: str) -> str:
    columns = set(columns_table_1).intersection(set(columns_table_2))
    columns = list(columns)
    columns.remove(primary_key)
    columns = ", ".join(columns)
    return f"""
        SELECT
            {columns},
            COUNT(*) AS total_rows,
            COUNTIF(table1.{primary_key} != table2.{primary_key}) AS diff_rows,
            ROUND(COUNTIF(table1.{primary_key} != table2.{primary_key}) / COUNT(*) * 100, 2) AS percentage_of_diff
        FROM `{table1}` AS table1
        FULL OUTER JOIN `{table2}` AS table2
        USING ({primary_key})
        GROUP BY {columns}
    """
