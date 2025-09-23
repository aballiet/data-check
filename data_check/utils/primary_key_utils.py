"""Utilities for handling primary key operations consistently across the codebase."""

from typing import List, Union, Any
from sqlglot.expressions import Select
from sqlglot import column, condition, func


class PrimaryKeyHandler:
    """Handles primary key operations with consistent logic for single and multiple keys."""

    def __init__(self, primary_keys: Union[str, List[str]]):
        """Initialize with primary key(s), ensuring consistent list format."""
        self._primary_keys = self._normalize_primary_keys(primary_keys)

    @staticmethod
    def _normalize_primary_keys(primary_keys: Union[str, List[str], None]) -> List[str]:
        """Normalize primary keys to a consistent list format."""
        if primary_keys is None:
            raise ValueError("Primary keys cannot be None")

        if isinstance(primary_keys, str):
            # Handle comma-separated string or single key
            if ',' in primary_keys:
                keys = [key.strip() for key in primary_keys.split(',') if key.strip()]
                if not keys:
                    raise ValueError("No valid primary keys found")
                return keys
            stripped = primary_keys.strip()
            if not stripped:
                raise ValueError("No valid primary keys found")
            return [stripped]

        if isinstance(primary_keys, list):
            if not primary_keys:
                raise ValueError("Primary keys list cannot be empty")
            # Flatten any nested lists and filter out empty strings
            flattened = []
            for key in primary_keys:
                if isinstance(key, str) and key.strip():
                    flattened.append(key.strip())
                elif isinstance(key, list):
                    flattened.extend([k.strip() for k in key if isinstance(k, str) and k.strip()])
            if not flattened:
                raise ValueError("No valid primary keys found")
            return flattened

        raise ValueError(f"Primary keys must be string or list, got {type(primary_keys)}")

    @property
    def keys(self) -> List[str]:
        """Get the normalized primary keys as a list."""
        return self._primary_keys

    @property
    def is_single_key(self) -> bool:
        """Check if this is a single primary key."""
        return len(self._primary_keys) == 1

    @property
    def is_composite_key(self) -> bool:
        """Check if this is a composite (multiple) primary key."""
        return len(self._primary_keys) > 1

    def get_single_key(self) -> str:
        """Get the single primary key. Raises error if composite."""
        if not self.is_single_key:
            raise ValueError("Cannot get single key from composite primary key")
        return self._primary_keys[0]

    def get_concat_expression(self, table_prefix: str = "") -> str:
        """Get concatenated primary key expression for SQL queries."""
        if self.is_single_key:
            prefix = f"{table_prefix}." if table_prefix else ""
            return f"{prefix}{self._primary_keys[0]}"

        # For multiple primary keys, concatenate them as strings with null handling
        prefix = f"{table_prefix}." if table_prefix else ""
        pk_exprs = [f"coalesce(cast({prefix}{pk} as string), '')" for pk in self._primary_keys]
        return f"concat({', '.join(pk_exprs)})"

    def get_join_condition(self, table1_alias: str = "table1", table2_alias: str = "table2") -> str:
        """Get join condition for primary keys between two tables."""
        if self.is_single_key:
            pk = self._primary_keys[0]
            return f"{table1_alias}.{pk} = {table2_alias}.{pk}"

        # For composite keys, compare concatenated expressions
        table1_concat = self.get_concat_expression(table1_alias)
        table2_concat = self.get_concat_expression(table2_alias)
        return f"{table1_concat} = {table2_concat}"

    def get_null_check_condition(self, table_alias: str) -> str:
        """Get condition to check for null/empty primary keys."""
        concat_expr = self.get_concat_expression(table_alias)

        if self.is_single_key:
            return f"{concat_expr} is null or {concat_expr} = ''"
        else:
            return f"{concat_expr} is null or {concat_expr} = ''"

    def get_group_by_columns(self, table_alias: str = "") -> List[str]:
        """Get columns for GROUP BY clause."""
        if table_alias:
            return [f"{table_alias}.{pk}" for pk in self._primary_keys]
        return self._primary_keys

    def get_select_columns(self, table_alias: str = "") -> List[column]:
        """Get sqlglot column expressions for SELECT clause."""
        if table_alias:
            return [column(pk, table=table_alias) for pk in self._primary_keys]
        return [column(pk) for pk in self._primary_keys]

    def validate_uniqueness_query(self, table_name: str, base_query: Select) -> Select:
        """Generate query to validate primary key uniqueness."""
        return (
            base_query
            .select(func("count", "*").as_("total_rows"))
            .from_(table_name)
            .group_by(*self._primary_keys)
            .having(func("count", "*") > 1)
        )

    def __str__(self) -> str:
        """String representation of primary keys."""
        if self.is_single_key:
            return self._primary_keys[0]
        return f"({', '.join(self._primary_keys)})"

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"PrimaryKeyHandler({self._primary_keys})"

    def __eq__(self, other) -> bool:
        """Check equality with another PrimaryKeyHandler."""
        if not isinstance(other, PrimaryKeyHandler):
            return False
        return self._primary_keys == other._primary_keys

    def __hash__(self) -> int:
        """Make PrimaryKeyHandler hashable for use in sets/dicts."""
        return hash(tuple(self._primary_keys))


def create_primary_key_handler(primary_keys: Union[str, List[str]]) -> PrimaryKeyHandler:
    """Factory function to create a PrimaryKeyHandler."""
    return PrimaryKeyHandler(primary_keys)


def validate_primary_keys(primary_keys: Union[str, List[str]]) -> List[str]:
    """Validate and normalize primary keys without creating a handler."""
    return PrimaryKeyHandler._normalize_primary_keys(primary_keys)


def is_valid_primary_key_format(primary_keys: Any) -> bool:
    """Check if the provided value is a valid primary key format."""
    try:
        PrimaryKeyHandler._normalize_primary_keys(primary_keys)
        return True
    except (ValueError, TypeError):
        return False


# Common primary key patterns for testing
COMMON_PK_PATTERNS = {
    'single_string': 'id',
    'single_list': ['id'],
    'composite_list': ['customer_id', 'order_id'],
    'comma_separated': 'customer_id,order_id',
    'spaced_comma_separated': 'customer_id, order_id',
}

# Edge cases that should be handled
EDGE_CASE_PATTERNS = {
    'empty_list': [],
    'none_value': None,
    'empty_string': '',
    'whitespace_only': '   ',
    'mixed_types': ['id', 123],  # This should raise an error
    'nested_list': [['id']],  # This should be flattened
}