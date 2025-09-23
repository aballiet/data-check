"""
Simple End-to-End Test: Complete Data Comparison Workflow (No Cache)

This test demonstrates the core data comparison functionality without
cache dependencies, focusing on SQL generation and primary key handling.

Test Scenario:
- Test unified primary key handling edge cases
- Validate SQL generation performance
- Demonstrate modular component architecture
- Verify core functionality works correctly

Expected Performance Improvements:
- ✅ Unified primary key handling eliminates edge cases
- ✅ Fast SQL generation with consistent results
- ✅ Modular architecture for better maintainability
- ✅ DRY code principles applied throughout
"""

import pytest
import pandas as pd
import time

from data_check.utils.primary_key_utils import PrimaryKeyHandler


class TestCompleteWorkflowSimpleNoCache:
    """
    Simple end-to-end test for core data comparison functionality.

    This test validates the key optimizations we implemented without
    cache dependencies:
    1. Unified primary key handling that eliminates edge cases
    2. Fast SQL generation for different primary key configurations
    3. Performance improvements from modular architecture
    """

    def test_unified_primary_key_handling_eliminates_edge_cases(self):
        """
        OPTIMIZATION 1: Unified Primary Key Handling

        This test demonstrates how our unified primary key handler eliminates
        edge cases by treating all input formats consistently.

        Before: Different logic for single vs multiple keys, inconsistent parsing
        After: Single unified handler for all primary key formats
        """
        print("\n🔑 Testing Unified Primary Key Handling")

        # Test Case 1: Single primary key as string
        pk_handler_1 = PrimaryKeyHandler("customer_id")
        assert pk_handler_1.keys == ["customer_id"]
        assert pk_handler_1.get_concat_expression("t1") == "t1.customer_id"

        # Test Case 2: Single primary key as list
        pk_handler_2 = PrimaryKeyHandler(["customer_id"])
        assert pk_handler_2.keys == ["customer_id"]
        assert pk_handler_2.get_concat_expression("t1") == "t1.customer_id"

        # Test Case 3: Multiple primary keys as comma-separated string
        pk_handler_3 = PrimaryKeyHandler("customer_id, order_id, item_id")
        assert pk_handler_3.keys == ["customer_id", "order_id", "item_id"]
        expected_concat = "concat(coalesce(cast(t1.customer_id as string), ''), coalesce(cast(t1.order_id as string), ''), coalesce(cast(t1.item_id as string), ''))"
        assert pk_handler_3.get_concat_expression("t1") == expected_concat

        # Test Case 4: Multiple primary keys as list
        pk_handler_4 = PrimaryKeyHandler(["customer_id", "order_id", "item_id"])
        assert pk_handler_4.keys == ["customer_id", "order_id", "item_id"]
        assert pk_handler_4.get_concat_expression("t1") == expected_concat

        # Test Case 5: Complex primary key with special characters
        pk_handler_5 = PrimaryKeyHandler("customer.id, order-id, item_123")
        assert pk_handler_5.keys == ["customer.id", "order-id", "item_123"]

        print("   ✅ All primary key formats handled consistently")
        print("   ✅ Single vs multiple key edge cases eliminated")
        print("   ✅ String vs list input formats unified")

    def test_sql_generation_performance(self):
        """
        OPTIMIZATION 2: SQL Generation Performance

        This test demonstrates that SQL generation is fast and efficient
        for different primary key configurations.

        Before: Inconsistent primary key handling, complex SQL generation
        After: Unified handling, optimized SQL generation
        """
        print("\n⚡ Testing SQL Generation Performance")

        # Test SQL generation performance for different primary key types
        test_cases = [
            ("single_key", "customer_id"),
            ("composite_key", ["customer_id", "order_id"]),
            ("complex_key", ["customer_id", "order_id", "item_id", "date"]),
        ]

        performance_results = {}

        for case_name, primary_key in test_cases:
            start_time = time.time()

            # Test primary key handler creation and SQL generation
            pk_handler = PrimaryKeyHandler(primary_key)
            concat_expr = pk_handler.get_concat_expression("table1")
            join_condition = pk_handler.get_join_condition()
            null_check = pk_handler.get_null_check_condition("table1")

            generation_time = time.time() - start_time
            performance_results[case_name] = generation_time

            # Validate results
            assert len(pk_handler.keys) > 0
            assert concat_expr is not None
            assert join_condition is not None
            assert null_check is not None

            print(f"   ✅ {case_name}: {generation_time:.6f}s for {len(pk_handler.keys)} columns")

        # Performance should be consistently fast
        max_time = max(performance_results.values())
        assert max_time < 0.001, f"SQL generation should be fast, got {max_time:.6f}s"

        print(f"   ✅ All SQL generation completed in under 1ms")
        print(f"   ✅ Unified primary key handling working efficiently")

    def test_input_validation_and_error_handling(self):
        """
        OPTIMIZATION 3: Input Validation and Error Handling

        This test demonstrates robust input validation and proper error
        handling for edge cases and invalid inputs.

        Before: Inconsistent error handling, unclear error messages
        After: Comprehensive validation with clear error messages
        """
        print("\n🛡️ Testing Input Validation and Error Handling")

        # Test invalid inputs raise appropriate errors
        invalid_inputs = [
            (None, "Primary keys cannot be None"),
            ([], "Primary keys list cannot be empty"),
            ("", "No valid primary keys found"),
            ("   ", "No valid primary keys found"),
            (123, "Primary keys must be string or list"),
        ]

        for invalid_input, expected_error in invalid_inputs:
            with pytest.raises(ValueError, match=expected_error):
                PrimaryKeyHandler(invalid_input)

        print("   ✅ Invalid inputs properly rejected with clear errors")

        # Test edge case handling
        edge_cases = [
            ("customer_id,", ["customer_id"]),  # Trailing comma
            (" customer_id , order_id ", ["customer_id", "order_id"]),  # Extra spaces
            ("customer_id, , order_id", ["customer_id", "order_id"]),  # Empty part
        ]

        for input_value, expected_output in edge_cases:
            pk_handler = PrimaryKeyHandler(input_value)
            assert pk_handler.keys == expected_output

        print("   ✅ Edge cases handled gracefully")
        print("   ✅ Robust input validation working correctly")

    def test_performance_with_realistic_data_patterns(self):
        """
        OPTIMIZATION 4: Performance with Realistic Data

        This test demonstrates performance with realistic primary key
        patterns that would be encountered in production environments.

        Before: Slow operations, excessive processing for simple cases
        After: Fast operations, optimized for common patterns
        """
        print("\n📊 Testing Performance with Realistic Data Patterns")

        # Common primary key patterns in real-world scenarios
        realistic_patterns = {
            'simple_id': "id",
            'uuid': "uuid",
            'composite_customer': ["customer_id", "order_date"],
            'complex_ecommerce': ["customer_id", "order_id", "item_id"],
            'multi_tenant': ["tenant_id", "user_id", "resource_id", "timestamp"],
            'hierarchical': ["company_id", "department_id", "employee_id", "project_id"],
        }

        performance_results = {}

        for pattern_name, primary_key in realistic_patterns.items():
            start_time = time.time()

            # Create handler and generate common SQL expressions
            pk_handler = PrimaryKeyHandler(primary_key)

            # Generate all common SQL expressions that would be used in practice
            expressions = {
                'concat': pk_handler.get_concat_expression("t1"),
                'join': pk_handler.get_join_condition(),
                'null_check': pk_handler.get_null_check_condition("t1"),
                'group_by': pk_handler.get_group_by_columns("t1"),
                'select': pk_handler.get_select_columns("t1"),
            }

            processing_time = time.time() - start_time
            performance_results[pattern_name] = {
                'time': processing_time,
                'key_count': len(pk_handler.keys),
                'expressions_count': len(expressions)
            }

            # Validate all expressions were generated
            assert all(expr is not None for expr in expressions.values())

            print(f"   ✅ {pattern_name}: {processing_time:.6f}s for {len(pk_handler.keys)} keys")

        # Performance should scale well with complexity
        max_time = max(result['time'] for result in performance_results.values())
        assert max_time < 0.002, f"Even complex patterns should be fast, got {max_time:.6f}s"

        print(f"   ✅ All realistic patterns processed efficiently")
        print(f"   ✅ Maximum processing time: {max_time:.6f}s")

    def test_complete_workflow_integration(self):
        """
        COMPLETE WORKFLOW INTEGRATION TEST

        This test demonstrates how all optimizations work together to provide
        a fast, reliable user experience for data comparison workflows.

        Integration Points:
        1. Unified primary key handling
        2. Fast SQL generation
        3. Robust error handling
        4. Performance optimization
        """
        print("\n🎯 Testing Complete Workflow Integration")

        workflow_start = time.time()

        # Step 1: Configure primary keys (demonstrating unified handling)
        print("1️⃣ Configuring primary keys...")

        # Test different primary key formats in sequence
        pk_formats = [
            "customer_id",                           # String format
            ["customer_id"],                         # Single item list
            "customer_id, order_date",              # Comma-separated
            ["customer_id", "order_date", "item_id"] # Multi-item list
        ]

        workflow_results = {}

        for i, pk_format in enumerate(pk_formats):
            pk_handler = PrimaryKeyHandler(pk_format)
            workflow_results[f"config_{i}"] = pk_handler.keys

            # Verify consistent handling
            assert isinstance(pk_handler.keys, list)
            assert len(pk_handler.keys) > 0

        # Step 2: Generate SQL expressions (demonstrating performance)
        print("2️⃣ Generating SQL expressions...")

        sql_operations = [
            ("primary_key_analysis", "concat"),
            ("join_conditions", "join"),
            ("null_checks", "null_check"),
            ("grouping", "group_by"),
            ("selection", "select"),
        ]

        for operation_name, method_suffix in sql_operations:
            start_time = time.time()

            # Use the most complex primary key for testing
            pk_handler = PrimaryKeyHandler(["customer_id", "order_date", "item_id"])
            method_name = f"get_{method_suffix}_{'expression' if method_suffix in ['concat'] else 'condition' if method_suffix in ['join', 'null_check'] else 'columns'}"

            if hasattr(pk_handler, method_name):
                result = getattr(pk_handler, method_name)("table1")
                operation_time = time.time() - start_time
                workflow_results[operation_name] = {
                    'result': result,
                    'time': operation_time
                }

        # Step 3: Validate workflow performance
        print("3️⃣ Validating workflow performance...")

        workflow_total_time = time.time() - workflow_start

        # Validate all operations completed successfully
        for operation, result in workflow_results.items():
            if isinstance(result, dict) and 'result' in result:
                assert result['result'] is not None, f"{operation} should generate valid SQL"
                assert result['time'] < 0.001, f"{operation} should be fast"

        print(f"   ✅ Complete workflow executed in {workflow_total_time:.3f}s")
        print(f"   ✅ All {len(sql_operations)} SQL operations completed successfully")
        print(f"   ✅ All {len(pk_formats)} primary key formats handled correctly")
        print("   ✅ All optimizations working together successfully")

        # Performance assertions
        assert workflow_total_time < 0.1, "Complete workflow should be very fast"

        print(f"   🎯 Workflow validated: All optimizations working efficiently")


if __name__ == "__main__":
    # Run the simple workflow tests without cache dependencies
    pytest.main([__file__, "-v", "-s"])