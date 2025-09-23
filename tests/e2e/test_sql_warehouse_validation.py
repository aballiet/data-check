"""
SQL Warehouse Validation Tests

This test suite validates that SQL queries generated from form data are correct
and suitable for BigQuery warehouse execution. Focuses on practical validation
of queries that users will actually run.

Test Coverage:
- ✅ Primary key analysis queries
- ✅ Schema comparison queries
- ✅ Data difference detection
- ✅ Query syntax validation
- ✅ BigQuery compatibility
"""

import pytest
from unittest.mock import Mock, patch

from data_check.processors.bigquery import BigQueryProcessor
from data_check.models.table import TableSchema, ColumnSchema, BigQueryDataType


class TestSQLWarehouseValidation:
    """
    Practical SQL validation tests for warehouse queries.

    These tests ensure generated SQL is ready for production BigQuery execution.
    """

    @pytest.fixture
    def configured_processor(self):
        """Create a properly configured BigQuery processor."""
        with patch('data_check.processors.bigquery.QueryBigQuery'):
            processor = BigQueryProcessor(
                "SELECT * FROM `project.dataset.customers_2023`",
                "SELECT * FROM `project.dataset.customers_2024`"
            )

            processor.set_config_data(
                primary_key=["customer_id"],
                columns_to_compare=["email", "phone", "status"],
                sampling_rate=100
            )

            return processor

    @pytest.fixture
    def sample_schema(self):
        """Create sample table schema."""
        return TableSchema("customers", [
            ColumnSchema("customer_id", BigQueryDataType.INTEGER, None),
            ColumnSchema("email", BigQueryDataType.STRING, None),
            ColumnSchema("phone", BigQueryDataType.STRING, None),
            ColumnSchema("status", BigQueryDataType.STRING, None),
        ])

    def test_primary_key_insight_query_structure(self, configured_processor):
        """Test primary key insight query is properly structured."""
        print("\n🔑 Testing Primary Key Insight Query")

        query = configured_processor.get_query_insight_tables_primary_keys()
        sql = query.sql(pretty=True, dialect="bigquery")

        print(f"Generated SQL ({len(sql)} chars):")
        print(sql[:500] + "..." if len(sql) > 500 else sql)

        # Validate basic structure
        assert "WITH" in sql.upper(), "Should use WITH clause"
        assert "table1" in sql, "Should reference table1"
        assert "table2" in sql, "Should reference table2"
        assert "COUNT(*)" in sql.upper(), "Should count rows"
        assert "FULL OUTER JOIN" in sql.upper(), "Should use full outer join"

        # Validate BigQuery functions
        assert "SAFE_DIVIDE" in sql.upper(), "Should use SAFE_DIVIDE"
        assert "COUNTIF" in sql.upper(), "Should use COUNTIF"

        # Validate primary key handling
        assert "customer_id" in sql, "Should include primary key column"

        print("   ✅ Query structure validated")
        print("   ✅ BigQuery functions present")
        print("   ✅ Primary key properly handled")

    def test_primary_key_uniqueness_query(self, configured_processor):
        """Test primary key uniqueness validation query."""
        print("\n🔍 Testing Primary Key Uniqueness Query")

        query = configured_processor.get_query_check_primary_keys_unique("table1")
        sql = query.sql(pretty=True, dialect="bigquery")

        print(f"Generated SQL ({len(sql)} chars):")
        print(sql)

        # Validate uniqueness check structure
        assert "COUNT(*)" in sql.upper(), "Should count occurrences"
        assert "GROUP BY" in sql.upper(), "Should group by primary key"
        assert "HAVING" in sql.upper(), "Should filter duplicates"
        assert "COUNT(*) > 1" in sql.upper(), "Should find duplicates"
        assert "customer_id" in sql, "Should group by primary key column"

        print("   ✅ Uniqueness validation structure correct")
        print("   ✅ Proper duplicate detection logic")

    def test_composite_primary_key_handling(self, configured_processor):
        """Test handling of composite primary keys."""
        print("\n🔗 Testing Composite Primary Key Handling")

        # Reconfigure with composite key
        configured_processor.set_config_data(
            primary_key=["customer_id", "account_type"],
            columns_to_compare=["email", "status"],
            sampling_rate=100
        )

        query = configured_processor.get_query_insight_tables_primary_keys()
        sql = query.sql(pretty=True, dialect="bigquery")

        print(f"Generated SQL with composite key ({len(sql)} chars):")
        print(sql[:300] + "..." if len(sql) > 300 else sql)

        # Validate composite key handling
        pk_handler = configured_processor.pk_handler
        assert len(pk_handler.keys) == 2, "Should handle 2-column composite key"

        # Check for concatenation logic
        assert "concat" in sql.lower(), "Should concatenate composite key columns"
        assert "customer_id" in sql, "Should include first key column"
        assert "account_type" in sql, "Should include second key column"

        print(f"   ✅ Composite key with {len(pk_handler.keys)} columns handled")
        print("   ✅ Proper concatenation logic present")

    def test_column_ratio_query_generation(self, configured_processor, sample_schema):
        """Test column ratio analysis query generation."""
        print("\n📊 Testing Column Ratio Query Generation")

        query = configured_processor.query_ratio_common_values_per_column(sample_schema)
        sql = query.sql(pretty=True, dialect="bigquery")

        print(f"Generated SQL ({len(sql)} chars):")
        print(sql[:400] + "..." if len(sql) > 400 else sql)

        # Validate ratio analysis structure
        assert "COALESCE" in sql.upper(), "Should handle null values"
        assert "CAST" in sql.upper(), "Should cast for consistency"
        assert "SAFE_DIVIDE" in sql.upper(), "Should use safe division"
        assert "STRUCT" in sql.upper(), "Should create structured results"

        # Validate column analysis
        for column in ["email", "phone", "status"]:
            assert column in sql, f"Should analyze {column} column"

        print("   ✅ Ratio analysis structure validated")
        print("   ✅ All specified columns included")
        print("   ✅ Proper null handling and type casting")

    def test_row_difference_query_for_selected_columns(self, configured_processor, sample_schema):
        """Test row difference query for specific columns."""
        print("\n🔍 Testing Row Difference Query")

        # Test with filtered schema (mimicking user column selection)
        selected_columns = ["email", "status"]
        filtered_schema = TableSchema(
            table_name="filtered",
            columns=[sample_schema.get_column(col) for col in selected_columns]
        )

        query = configured_processor.get_query_plain_diff_tables(filtered_schema)
        sql = query.sql(pretty=True, dialect="bigquery")

        print(f"Generated SQL for columns {selected_columns} ({len(sql)} chars):")
        print(sql[:400] + "..." if len(sql) > 400 else sql)

        # Validate difference detection structure
        assert "__1" in sql, "Should alias table1 columns"
        assert "__2" in sql, "Should alias table2 columns"
        assert "WHERE" in sql.upper(), "Should filter for differences"

        # Validate selected columns are included
        for column in selected_columns:
            assert f"{column}__1" in sql, f"Should include {column} from table1"
            assert f"{column}__2" in sql, f"Should include {column} from table2"

        print(f"   ✅ Row difference query for {len(selected_columns)} columns")
        print("   ✅ Proper column aliasing and filtering")

    def test_query_syntax_validation(self, configured_processor):
        """Test that all generated queries have valid SQL syntax."""
        print("\n✅ Testing Query Syntax Validation")

        test_queries = [
            ("Primary Key Insight", configured_processor.get_query_insight_tables_primary_keys()),
            ("Uniqueness Check Table1", configured_processor.get_query_check_primary_keys_unique("table1")),
            ("Uniqueness Check Table2", configured_processor.get_query_check_primary_keys_unique("table2")),
        ]

        syntax_checks = {
            "balanced_parentheses": lambda sql: sql.count("(") == sql.count(")"),
            "has_select": lambda sql: "SELECT" in sql.upper(),
            "has_from": lambda sql: "FROM" in sql.upper(),
            "no_syntax_errors": lambda sql: not any(err in sql.upper() for err in ["SYNTAX ERROR", "INVALID"]),
            "proper_quotes": lambda sql: sql.count("'") % 2 == 0 if "'" in sql else True,
        }

        all_passed = True
        for query_name, query in test_queries:
            sql = query.sql(pretty=True, dialect="bigquery")

            print(f"   🔍 Validating {query_name}...")

            for check_name, check_func in syntax_checks.items():
                if not check_func(sql):
                    print(f"     ❌ Failed {check_name}")
                    all_passed = False
                else:
                    print(f"     ✅ Passed {check_name}")

        assert all_passed, "All syntax checks should pass"
        print(f"   ✅ All {len(test_queries)} queries passed syntax validation")

    def test_bigquery_compatibility(self, configured_processor):
        """Test BigQuery-specific compatibility."""
        print("\n🏗️ Testing BigQuery Compatibility")

        query = configured_processor.get_query_insight_tables_primary_keys()
        sql = query.sql(pretty=True, dialect="bigquery")

        # BigQuery compatibility checks
        bigquery_features = {
            "safe_functions": ["SAFE_DIVIDE"],
            "conditional_aggregates": ["COUNTIF"],
            "proper_joins": ["FULL OUTER JOIN"],
            "type_handling": ["CAST", "COALESCE"],
        }

        compatibility_score = 0
        for feature_category, features in bigquery_features.items():
            found_features = [f for f in features if f in sql.upper()]
            if found_features:
                compatibility_score += 1
                print(f"   ✅ {feature_category}: {', '.join(found_features)}")
            else:
                print(f"   ⚠️ {feature_category}: None found")

        # Assert minimum compatibility
        assert compatibility_score >= 3, f"Should have high BigQuery compatibility, got {compatibility_score}/4"
        print(f"   ✅ BigQuery compatibility score: {compatibility_score}/4")

    def test_realistic_user_workflow(self, configured_processor, sample_schema):
        """Test a realistic end-to-end user workflow."""
        print("\n🎯 Testing Realistic User Workflow")

        workflow_results = {}

        # Step 1: User configures comparison
        print("   1️⃣ User configures table comparison...")
        configured_processor.set_config_data(
            primary_key="customer_id",
            columns_to_compare=["email", "phone", "status"],
            sampling_rate=100
        )
        print("     ✅ Configuration complete")

        # Step 2: System analyzes primary keys
        print("   2️⃣ System analyzes primary keys...")
        pk_query = configured_processor.get_query_insight_tables_primary_keys()
        pk_sql = pk_query.sql(pretty=True, dialect="bigquery")
        workflow_results["primary_key_analysis"] = pk_sql
        assert "customer_id" in pk_sql, "Should analyze configured primary key"
        print("     ✅ Primary key analysis query generated")

        # Step 3: User selects specific columns for row differences
        print("   3️⃣ User selects columns for difference analysis...")
        selected_columns = ["email", "status"]  # User selection
        filtered_schema = TableSchema(
            table_name="user_selected",
            columns=[sample_schema.get_column(col) for col in selected_columns]
        )

        diff_query = configured_processor.get_query_plain_diff_tables(filtered_schema)
        diff_sql = diff_query.sql(pretty=True, dialect="bigquery")
        workflow_results["row_differences"] = diff_sql

        # Validate user-selected columns are properly handled
        for col in selected_columns:
            assert f"{col}__1" in diff_sql, f"Should include user-selected {col} from table1"
            assert f"{col}__2" in diff_sql, f"Should include user-selected {col} from table2"
        print(f"     ✅ Row difference query for {len(selected_columns)} user-selected columns")

        # Step 4: Validate all queries are warehouse-ready
        print("   4️⃣ Validating all queries are warehouse-ready...")
        for step_name, sql in workflow_results.items():
            # Basic warehouse readiness checks
            assert len(sql) > 50, f"{step_name} should be substantial query"
            assert len(sql) < 5000, f"{step_name} should not be overly complex"
            assert "SELECT" in sql.upper(), f"{step_name} should be valid SELECT"
            assert "FROM" in sql.upper(), f"{step_name} should have data source"

        print("     ✅ All generated queries are warehouse-ready")
        print(f"   🎯 Complete workflow generated {len(workflow_results)} valid queries")

        # Test completed successfully
        assert len(workflow_results) == 2, "Should generate exactly 2 queries in workflow"

    def test_form_data_to_sql_mapping(self, configured_processor):
        """Test that form data correctly maps to SQL generation."""
        print("\n📝 Testing Form Data to SQL Mapping")

        # Test different form configurations
        test_configurations = [
            {
                "name": "Single Primary Key",
                "primary_key": "customer_id",
                "columns": ["email", "phone"],
                "expected_in_sql": ["customer_id"]  # Primary key insight query only includes primary keys
            },
            {
                "name": "Composite Primary Key List",
                "primary_key": ["customer_id", "region"],
                "columns": ["status"],
                "expected_in_sql": ["customer_id", "region", "CONCAT"]  # Primary key insight includes PK columns + CONCAT
            },
            {
                "name": "Single Item List Primary Key",
                "primary_key": ["customer_id"],
                "columns": ["email"],
                "expected_in_sql": ["customer_id"]  # Single item list should work like string
            }
        ]

        for config in test_configurations:
            print(f"   🧪 Testing {config['name']}...")

            # Configure processor with form data
            configured_processor.set_config_data(
                primary_key=config["primary_key"],
                columns_to_compare=config["columns"],
                sampling_rate=100
            )

            # Generate SQL
            query = configured_processor.get_query_insight_tables_primary_keys()
            sql = query.sql(pretty=True, dialect="bigquery")

            # Validate form data appears in SQL
            for expected_element in config["expected_in_sql"]:
                assert expected_element in sql, f"Should include {expected_element} from form data"

            print(f"     ✅ Form data correctly mapped to SQL")

        print(f"   ✅ Tested {len(test_configurations)} form configurations")


if __name__ == "__main__":
    # Run the warehouse validation tests
    pytest.main([__file__, "-v", "-s"])