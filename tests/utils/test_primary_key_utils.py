"""Comprehensive tests for primary key utilities."""

import pytest
from typing import List, Union

from data_check.utils.primary_key_utils import (
    PrimaryKeyHandler,
    create_primary_key_handler,
    validate_primary_keys,
    is_valid_primary_key_format,
    COMMON_PK_PATTERNS,
    EDGE_CASE_PATTERNS
)


class TestPrimaryKeyHandler:
    """Test cases for PrimaryKeyHandler class."""

    def test_single_string_key(self):
        """Test handling of single string primary key."""
        handler = PrimaryKeyHandler("id")

        assert handler.keys == ["id"]
        assert handler.is_single_key
        assert not handler.is_composite_key
        assert handler.get_single_key() == "id"
        assert str(handler) == "id"

    def test_single_list_key(self):
        """Test handling of single key in list format."""
        handler = PrimaryKeyHandler(["id"])

        assert handler.keys == ["id"]
        assert handler.is_single_key
        assert not handler.is_composite_key
        assert handler.get_single_key() == "id"

    def test_composite_key_list(self):
        """Test handling of composite primary key as list."""
        handler = PrimaryKeyHandler(["customer_id", "order_id"])

        assert handler.keys == ["customer_id", "order_id"]
        assert not handler.is_single_key
        assert handler.is_composite_key
        assert str(handler) == "(customer_id, order_id)"

    def test_comma_separated_string(self):
        """Test handling of comma-separated primary keys."""
        handler = PrimaryKeyHandler("customer_id,order_id")

        assert handler.keys == ["customer_id", "order_id"]
        assert handler.is_composite_key

    def test_spaced_comma_separated_string(self):
        """Test handling of comma-separated keys with spaces."""
        handler = PrimaryKeyHandler("customer_id, order_id, item_id")

        assert handler.keys == ["customer_id", "order_id", "item_id"]
        assert len(handler.keys) == 3

    def test_get_single_key_from_composite_raises_error(self):
        """Test that getting single key from composite raises error."""
        handler = PrimaryKeyHandler(["id1", "id2"])

        with pytest.raises(ValueError, match="Cannot get single key from composite"):
            handler.get_single_key()

    def test_concat_expression_single_key(self):
        """Test concat expression for single primary key."""
        handler = PrimaryKeyHandler("id")

        assert handler.get_concat_expression("table1") == "table1.id"
        assert handler.get_concat_expression() == "id"

    def test_concat_expression_composite_key(self):
        """Test concat expression for composite primary key."""
        handler = PrimaryKeyHandler(["id", "date"])

        expr = handler.get_concat_expression("table1")
        expected = "concat(coalesce(cast(table1.id as string), ''), coalesce(cast(table1.date as string), ''))"
        assert expr == expected

    def test_join_condition_single_key(self):
        """Test join condition for single primary key."""
        handler = PrimaryKeyHandler("id")

        condition = handler.get_join_condition()
        assert condition == "table1.id = table2.id"

        condition_custom = handler.get_join_condition("t1", "t2")
        assert condition_custom == "t1.id = t2.id"

    def test_join_condition_composite_key(self):
        """Test join condition for composite primary key."""
        handler = PrimaryKeyHandler(["id", "date"])

        condition = handler.get_join_condition()
        # Should use concatenated expressions
        assert "concat" in condition
        assert "table1" in condition
        assert "table2" in condition

    def test_null_check_condition(self):
        """Test null check condition generation."""
        # Single key
        handler_single = PrimaryKeyHandler("id")
        condition = handler_single.get_null_check_condition("table1")
        assert condition == "table1.id is null or table1.id = ''"

        # Composite key
        handler_composite = PrimaryKeyHandler(["id", "date"])
        condition = handler_composite.get_null_check_condition("table1")
        assert "concat" in condition
        assert "is null or" in condition

    def test_group_by_columns(self):
        """Test group by columns generation."""
        handler = PrimaryKeyHandler(["id", "date"])

        columns = handler.get_group_by_columns()
        assert columns == ["id", "date"]

        columns_with_alias = handler.get_group_by_columns("t1")
        assert columns_with_alias == ["t1.id", "t1.date"]

    def test_select_columns(self):
        """Test select columns generation."""
        handler = PrimaryKeyHandler(["id", "date"])

        columns = handler.get_select_columns("table1")
        assert len(columns) == 2
        # Check that we get sqlglot column objects
        assert all(hasattr(col, 'table') for col in columns)

    def test_equality_and_hashing(self):
        """Test equality and hashing of PrimaryKeyHandler instances."""
        handler1 = PrimaryKeyHandler(["id", "date"])
        handler2 = PrimaryKeyHandler(["id", "date"])
        handler3 = PrimaryKeyHandler(["id", "name"])

        assert handler1 == handler2
        assert handler1 != handler3
        assert hash(handler1) == hash(handler2)
        assert hash(handler1) != hash(handler3)


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_none_primary_key_raises_error(self):
        """Test that None primary key raises ValueError."""
        with pytest.raises(ValueError, match="Primary keys cannot be None"):
            PrimaryKeyHandler(None)

    def test_empty_list_raises_error(self):
        """Test that empty list raises ValueError."""
        with pytest.raises(ValueError, match="Primary keys list cannot be empty"):
            PrimaryKeyHandler([])

    def test_empty_string_raises_error(self):
        """Test that empty string raises ValueError."""
        with pytest.raises(ValueError, match="No valid primary keys found"):
            PrimaryKeyHandler("")

    def test_whitespace_only_raises_error(self):
        """Test that whitespace-only string raises ValueError."""
        with pytest.raises(ValueError, match="No valid primary keys found"):
            PrimaryKeyHandler("   ")

    def test_invalid_type_raises_error(self):
        """Test that invalid types raise ValueError."""
        with pytest.raises(ValueError, match="Primary keys must be string or list"):
            PrimaryKeyHandler(123)

    def test_nested_list_flattening(self):
        """Test that nested lists are properly flattened."""
        handler = PrimaryKeyHandler([["id"], "date"])
        assert handler.keys == ["id", "date"]

    def test_mixed_valid_invalid_in_list(self):
        """Test handling of mixed valid/invalid items in list."""
        # This should extract only valid string keys
        handler = PrimaryKeyHandler(["id", "", "date", "   "])
        assert handler.keys == ["id", "date"]

    def test_comma_separated_with_empty_parts(self):
        """Test comma-separated string with empty parts."""
        handler = PrimaryKeyHandler("id,,date, ,name")
        assert handler.keys == ["id", "date", "name"]


class TestFactoryFunctions:
    """Test factory and utility functions."""

    def test_create_primary_key_handler(self):
        """Test factory function."""
        handler = create_primary_key_handler("id")
        assert isinstance(handler, PrimaryKeyHandler)
        assert handler.keys == ["id"]

    def test_validate_primary_keys(self):
        """Test validation function."""
        result = validate_primary_keys(["id", "date"])
        assert result == ["id", "date"]

        with pytest.raises(ValueError):
            validate_primary_keys(None)

    def test_is_valid_primary_key_format(self):
        """Test validation checker function."""
        assert is_valid_primary_key_format("id")
        assert is_valid_primary_key_format(["id", "date"])
        assert is_valid_primary_key_format("id,date")

        assert not is_valid_primary_key_format(None)
        assert not is_valid_primary_key_format([])
        assert not is_valid_primary_key_format(123)


class TestCommonPatterns:
    """Test common primary key patterns."""

    @pytest.mark.parametrize("pattern_name,pattern_value", COMMON_PK_PATTERNS.items())
    def test_common_patterns(self, pattern_name, pattern_value):
        """Test that all common patterns are handled correctly."""
        handler = PrimaryKeyHandler(pattern_value)
        assert len(handler.keys) > 0
        assert all(isinstance(key, str) and key.strip() for key in handler.keys)

    def test_single_string_pattern(self):
        """Test specific single string pattern."""
        handler = PrimaryKeyHandler(COMMON_PK_PATTERNS['single_string'])
        assert handler.is_single_key
        assert handler.get_single_key() == 'id'

    def test_composite_list_pattern(self):
        """Test specific composite list pattern."""
        handler = PrimaryKeyHandler(COMMON_PK_PATTERNS['composite_list'])
        assert handler.is_composite_key
        assert set(handler.keys) == {'customer_id', 'order_id'}


class TestErrorHandling:
    """Test error handling for edge cases."""

    @pytest.mark.parametrize("pattern_name,pattern_value", [
        (k, v) for k, v in EDGE_CASE_PATTERNS.items()
        if k not in ['mixed_types', 'nested_list']  # These have special handling
    ])
    def test_edge_case_patterns_raise_errors(self, pattern_name, pattern_value):
        """Test that edge case patterns raise appropriate errors."""
        with pytest.raises(ValueError):
            PrimaryKeyHandler(pattern_value)

    def test_nested_list_edge_case_handled(self):
        """Test that nested list edge case is handled properly."""
        # This should work by flattening
        handler = PrimaryKeyHandler(EDGE_CASE_PATTERNS['nested_list'])
        assert handler.keys == ['id']


class TestPerformanceAndConsistency:
    """Test performance and consistency across operations."""

    def test_consistent_behavior_different_inputs_same_result(self):
        """Test that different input formats produce same result."""
        inputs = [
            "id,date",
            "id, date",
            ["id", "date"],
            [["id"], "date"]
        ]

        handlers = [PrimaryKeyHandler(inp) for inp in inputs]

        # All should have same keys
        expected_keys = ["id", "date"]
        for handler in handlers:
            assert handler.keys == expected_keys

        # All should generate same join conditions
        join_conditions = [h.get_join_condition() for h in handlers]
        assert all(cond == join_conditions[0] for cond in join_conditions)

    def test_handler_immutability(self):
        """Test that handler state doesn't change unexpectedly."""
        handler = PrimaryKeyHandler(["id", "date"])
        original_keys = handler.keys.copy()

        # Generate various expressions
        _ = handler.get_concat_expression("table1")
        _ = handler.get_join_condition()
        _ = handler.get_null_check_condition("table1")

        # Keys should remain unchanged
        assert handler.keys == original_keys

    def test_repr_and_str_consistency(self):
        """Test string representations are consistent."""
        handler = PrimaryKeyHandler(["id", "date"])

        # repr should be informative for debugging
        repr_str = repr(handler)
        assert "PrimaryKeyHandler" in repr_str
        assert "id" in repr_str
        assert "date" in repr_str

        # str should be human-readable
        str_repr = str(handler)
        assert "id" in str_repr
        assert "date" in str_repr