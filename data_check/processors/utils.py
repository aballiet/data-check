from sqlglot import select, func, alias, condition, column
from sqlglot.expressions import Select, Alias
from typing import List


def add_suffix_to_column_names(table_name, column_names: List[str],  suffix: str) -> List[Alias]:
    """Add a suffix to all column names in a query"""
    return [
        alias(column(col, table=table_name), f"{col}{suffix}")
        for col in column_names
    ]