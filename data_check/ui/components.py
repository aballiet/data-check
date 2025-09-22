"""Modular UI components for the Streamlit data comparison app."""

from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import streamlit as st

from data_check.processors.bigquery import BigQueryProcessor
from data_check.data_formatter import highlight_diff_dataset, style_gradient, style_percentage
# Cache manager removed - using simple direct execution


class TableSelectionForm:
    """Component for table/query selection and configuration."""

    def __init__(self):
        self.form_key = "table_selection_form"

    def render(self) -> None:
        """Render the table selection form."""
        st.text_area(
            "Table or SQL Query 1",
            value=st.session_state.get("table1", ""),
            key="temp_table_1",
        )

        st.text_area(
            "Table or SQL Query 2",
            value=st.session_state.get("table2", ""),
            key="temp_table_2",
        )

        if st.form_submit_button(label="Configure Tables", on_click=self._on_submit):
            return True
        return False

    def _on_submit(self) -> None:
        """Handle form submission."""
        # Check if tables changed to invalidate caches
        old_table1 = st.session_state.get("table1")
        old_table2 = st.session_state.get("table2")
        new_table1 = st.session_state.temp_table_1
        new_table2 = st.session_state.temp_table_2

        if old_table1 != new_table1 or old_table2 != new_table2:
            # Cache invalidation removed - tables changed
            pass

        st.session_state.table1 = new_table1
        st.session_state.table2 = new_table2
        st.session_state.config_tables = True
        st.session_state.loaded_tables = False
        st.session_state.is_select_all = None
        st.session_state.columns_to_compare = None

        # Update query params
        st.query_params["table1"] = new_table1
        st.query_params["table2"] = new_table2


class ColumnConfigurationForm:
    """Component for primary key and column selection."""

    def __init__(self, processor: BigQueryProcessor):
        self.processor = processor
        self.form_key = "column_configuration_form"

    def _get_schema_analysis(self) -> Dict[str, Any]:
        """Get schema analysis with caching."""
        common_table_schema = self.processor.get_common_schema_from_tables()
        diff_columns1, diff_columns2 = self.processor.get_diff_columns()

        return {
            "common_schema": common_table_schema,
            "diff_columns1": diff_columns1,
            "diff_columns2": diff_columns2
        }

    def render(self) -> bool:
        """Render the column configuration form."""
        schema_analysis = self._get_schema_analysis()
        common_table_schema = schema_analysis["common_schema"]
        diff_columns1 = schema_analysis["diff_columns1"]
        diff_columns2 = schema_analysis["diff_columns2"]

        # Store schema in session state for later use
        st.session_state.common_table_schema = common_table_schema

        # Display schema differences
        if not diff_columns1.empty:
            st.write("Columns exclusive to table 1:")
            st.dataframe(diff_columns1, width=1400)

        if not diff_columns2.empty:
            st.write("Columns exclusive to table 2:")
            st.dataframe(diff_columns2, width=1400)

        # Primary key selection
        current_primary_keys = self._get_current_primary_keys(common_table_schema)
        st.multiselect(
            "Select primary key(s) (combination must be unique for a given row):",
            common_table_schema.columns_names,
            key="temp_primary_key",
            default=current_primary_keys,
        )

        # Column selection
        current_columns = st.session_state.get("columns_to_compare", [])
        if current_columns and not all(col in common_table_schema.columns_names for col in current_columns):
            current_columns = []  # Reset if invalid columns

        st.multiselect(
            "Select columns to compare:",
            common_table_schema.columns_names,
            key="temp_columns_to_compare",
            default=current_columns,
        )

        # Select all checkbox
        st.checkbox(
            "Select all",
            key="temp_is_select_all",
            value=st.session_state.get("is_select_all", False),
        )

        # Sampling rate slider
        sampling_disabled = not self.processor.is_sampling_allowed
        st.slider(
            "Data sampling (only available for direct tables as input)",
            min_value=10,
            max_value=100,
            step=1,
            key="temp_sampling_rate",
            value=100 if sampling_disabled else st.session_state.get("sampling_rate", 100),
            disabled=sampling_disabled,
        )

        if st.form_submit_button(label="Configure Columns", on_click=self._on_submit):
            return True
        return False

    def _get_current_primary_keys(self, common_table_schema) -> List[str]:
        """Get current primary keys filtered by valid columns."""
        current_primary_keys = []
        primary_key = st.session_state.get("primary_key")
        if primary_key is not None:
            current_primary_keys = [
                pk for pk in primary_key
                if pk in common_table_schema.columns_names
            ]
        return current_primary_keys

    def _on_submit(self) -> None:
        """Handle form submission with validation."""
        # Validate primary key selection
        temp_primary_key = st.session_state.get("temp_primary_key", [])
        if not temp_primary_key or len(temp_primary_key) == 0:
            st.error("Please select at least one primary key.")
            return

        # Check if primary key changed to invalidate caches
        old_primary_key = st.session_state.get("primary_key", [])
        if old_primary_key != temp_primary_key:
            # Cache invalidation removed - primary key changed
            pass

        # Update session state
        st.session_state.is_select_all = st.session_state.temp_is_select_all
        st.session_state.primary_key = temp_primary_key
        st.session_state.sampling_rate = st.session_state.temp_sampling_rate

        if st.session_state.is_select_all:
            st.session_state.columns_to_compare = st.session_state.common_table_schema.columns_names
        else:
            st.session_state.columns_to_compare = st.session_state.temp_columns_to_compare

        # Update query params
        st.query_params["sampling_rate"] = st.session_state.sampling_rate
        st.query_params["primary_key"] = ",".join(st.session_state.primary_key)
        st.query_params["columns_to_compare"] = ",".join(st.session_state.columns_to_compare)
        st.query_params["select_all"] = st.session_state.is_select_all
        st.query_params["table1"] = st.session_state.table1
        st.query_params["table2"] = st.session_state.table2

        st.session_state.loaded_tables = True


class PrimaryKeyAnalysisComponent:
    """Component for primary key analysis and validation."""

    def __init__(self, processor: BigQueryProcessor):
        self.processor = processor

    def render(self) -> bool:
        """Render primary key analysis."""
        if not self._validate_primary_key_uniqueness():
            return False

        st.write("Primary keys are unique for a given row ✅")

        # Analyze primary key overlap
        self._render_primary_key_overlap()
        return True

    
    def _validate_primary_key_uniqueness(self) -> bool:
        """Validate primary key uniqueness."""
        st.write("Checking primary keys are unique for a given row...")

        primary_keys_unique_table1, error_message_table1 = (
            self.processor.run_query_check_primary_keys_unique(table="table1")
        )
        primary_keys_unique_table2, error_message_table2 = (
            self.processor.run_query_check_primary_keys_unique(table="table2")
        )

        if not primary_keys_unique_table1 or not primary_keys_unique_table2:
            st.write("Primary keys are not unique for a given row ❌")
            if error_message_table1:
                st.write(error_message_table1)
            if error_message_table2:
                st.write(error_message_table2)
            st.stop()
            return False

        return True

    
    def _render_primary_key_overlap(self) -> None:
        """Render primary key overlap analysis."""
        st.write("Analyzing primary keys...")
        results_primary_keys = self.processor.run_query_compare_primary_keys()

        styled_results = style_percentage(
            results_primary_keys, columns=["missing_primary_keys_ratio"]
        )
        st.dataframe(styled_results)

        # Show exclusive primary keys if there are missing ones
        missing_ratio = results_primary_keys["missing_primary_keys_ratio"].iloc[0]
        if missing_ratio > 0:
            self._render_exclusive_primary_keys_section(missing_ratio)

    def _render_exclusive_primary_keys_section(self, missing_ratio: float) -> None:
        """Render section for exclusive primary keys."""
        button_key = f"show_exclusive_keys_{hash(str(missing_ratio))}"

        if st.button("Display exclusive primary keys for each table", key=button_key):
            st.write("Displaying rows where primary keys are different...")

            with st.spinner("Loading exclusive primary keys..."):
                df_exclusive_table1, df_exclusive_table2 = (
                    self.processor.run_query_exclusive_primary_keys()
                )

            st.write("Exclusive to table 1 (showing first 500 rows):")
            st.dataframe(df_exclusive_table1)

            st.write("Exclusive to table 2 (showing first 500 rows):")
            st.dataframe(df_exclusive_table2)


class ColumnDifferenceAnalysisComponent:
    """Component for column-level difference analysis."""

    def __init__(self, processor: BigQueryProcessor):
        self.processor = processor

    def render(self) -> Optional[List[str]]:
        """Render column difference analysis and return selected columns."""
        st.write("Computing difference ratio...")

        # Get and validate results
        results_ratio_per_column = self._get_column_diff_ratios()
        if results_ratio_per_column is None:
            return None

        # Prepare and style the dataframe
        styled_df = self._prepare_styled_dataframe(results_ratio_per_column)

        # Render interactive data editor
        selected_columns = self._render_column_selector(styled_df, results_ratio_per_column)

        return selected_columns

    
    def _get_column_diff_ratios(self) -> Optional[pd.DataFrame]:
        """Get column difference ratios with caching."""
        try:
            results_ratio_per_column = self.processor.get_column_diff_ratios(
                selected_columns=st.session_state.columns_to_compare,
                common_table_schema=st.session_state.common_table_schema,
            )

            # Check if dataframe is empty
            if results_ratio_per_column["ratio_not_null"].isna().all():
                st.error(
                    "The SQL query did not return any rows. "
                    "Please double check that the SQL queries entered are returning rows."
                )
                st.stop()
        except Exception as e:
            st.error(f"❌ **Column analysis failed:** {str(e)}")
            st.write("**Please check your configuration and try again.**")
            st.write("**Common issues:**")
            st.write("- Invalid column names in comparison")
            st.write("- SQL syntax errors")
            st.write("- Missing table permissions")
            st.stop()
            return None

        return results_ratio_per_column

    def _prepare_styled_dataframe(self, results_ratio_per_column: pd.DataFrame) -> pd.DataFrame:
        """Prepare and style the dataframe for display."""
        # Add selection column
        results_ratio_per_column = results_ratio_per_column.copy()
        results_ratio_per_column.insert(0, "Select", False)

        # Apply styling
        styled_df = style_percentage(
            results_ratio_per_column,
            columns=["percentage_diff_values", "ratio_not_null", "ratio_equal"],
        )
        styled_df = style_gradient(styled_df, columns=["percentage_diff_values"])
        styled_df = style_gradient(
            styled_df, columns=["ratio_equal"], gradient_color="white,blue"
        )

        return styled_df

    def _render_column_selector(
        self, styled_df: pd.DataFrame, original_df: pd.DataFrame
    ) -> Optional[List[str]]:
        """Render the interactive column selector."""
        origin_columns = original_df.columns.tolist()

        # Get dataframe row-selections from user with st.data_editor
        edited_df = st.data_editor(
            data=styled_df,
            hide_index=True,
            column_config={"Select": st.column_config.CheckboxColumn(required=True)},
            disabled=origin_columns,
            key="column_selector"
        )

        df_selection = edited_df[edited_df.Select]
        if not df_selection.empty:
            return df_selection.column.tolist()

        return None


class RowDifferenceViewerComponent:
    """Component for viewing row-by-row differences."""

    def __init__(self, processor: BigQueryProcessor):
        self.processor = processor

    def render(self, selected_columns: List[str]) -> None:
        """Render row-by-row differences for selected columns."""
        if not selected_columns:
            return

        st.write(f"Displaying rows where {selected_columns} is different...")

        # Generate and show SQL query first (before execution)
        try:
            st.write("🔄 Generating SQL query...")
            query = self._generate_difference_query(selected_columns)
            st.write("✅ Query generated successfully!")

            self._render_sql_query(query)

            # Add a button to execute the query
            if st.button("🚀 Execute Query", help="Click to run the SQL query on BigQuery"):
                with st.spinner("Executing query..."):
                    try:
                        dataset = self._execute_difference_query(query)

                        if dataset.empty:
                            st.write("No difference found ✅")
                            st.dataframe(dataset)
                            return

                        # Render data with controls
                        self._render_data_with_controls(dataset, selected_columns)
                    except Exception as e:
                        st.error(f"❌ **Query execution failed:** {str(e)}")
                        st.write("**Please check your query and try again.**")
                        st.write("**Common issues:**")
                        st.write("- Invalid column names")
                        st.write("- Syntax errors in SQL")
                        st.write("- Missing table permissions")
                        st.write("- Network connectivity issues")
            else:
                st.info("👆 Click 'Execute Query' above to run this SQL query and see the results")

        except Exception as e:
            st.error(f"Error generating SQL query: {str(e)}")
            st.exception(e)  # Show full stack trace for debugging
            return

    def _generate_difference_query(self, selected_columns: List[str]):
        """Generate the SQL query for row differences without executing it."""
        from data_check.models.table import TableSchema

        # Filter schema to only selected columns (same logic as get_plain_diff)
        filtered_columns = TableSchema(
            table_name="filtered_columns",
            columns=[
                st.session_state.common_table_schema.get_column(column) for column in selected_columns
            ],
        )

        return self.processor.get_query_plain_diff_tables(
            common_table_schema=filtered_columns,
        )

    
    def _execute_difference_query(self, query) -> pd.DataFrame:
        """Execute the difference query and return the dataset."""
        return self.processor.client.run_query_to_dataframe(query)


    def _render_data_with_controls(self, dataset: pd.DataFrame, selected_columns: List[str]) -> None:
        """Render dataset with sorting and pagination controls."""
        # Sorting controls
        dataset = self._apply_sorting_controls(dataset)

        # Pagination controls
        paginated_data = self._apply_pagination_controls(dataset)

        # Display data with highlighting
        st.dataframe(
            data=highlight_diff_dataset(paginated_data, columns=selected_columns),
            use_container_width=True,
        )

    def _apply_sorting_controls(self, dataset: pd.DataFrame) -> pd.DataFrame:
        """Apply sorting controls to dataset."""
        top_menu = st.columns(3)

        with top_menu[0]:
            sort = st.radio(
                "Sort Data", options=["Yes", "No"], horizontal=True, index=1, key="sort_data"
            )

        if sort == "Yes":
            with top_menu[1]:
                sort_field = st.selectbox(
                    "Sort By", options=dataset.columns, key="sort_field"
                )
            with top_menu[2]:
                sort_direction = st.radio(
                    "Direction", options=["⬆️", "⬇️"], horizontal=True, key="sort_direction"
                )

            dataset = dataset.sort_values(
                by=sort_field,
                ascending=sort_direction == "⬆️",
                ignore_index=True,
            )

        return dataset

    def _apply_pagination_controls(self, dataset: pd.DataFrame) -> pd.DataFrame:
        """Apply pagination controls and return current page data."""
        bottom_menu = st.columns((4, 1, 1))

        with bottom_menu[2]:
            batch_size = st.selectbox(
                "Page Size", options=[25, 50, 100, 500], key="page_size"
            )

        with bottom_menu[1]:
            total_pages = max(1, int(len(dataset) / batch_size))
            current_page = st.number_input(
                "Page", min_value=1, max_value=total_pages, step=1, key="current_page"
            )

        with bottom_menu[0]:
            st.markdown(f"Page **{current_page}** of **{total_pages}**")

        # Get current page data
        start_idx = (current_page - 1) * batch_size
        end_idx = start_idx + batch_size
        return dataset.iloc[start_idx:end_idx]

    def _render_sql_query(self, query: Any) -> None:
        """Render the SQL query used for differences."""
        st.subheader("🔍 Generated SQL Query")
        st.info("**Preview**: This is the SQL query that will be executed on BigQuery to find row differences.")

        # Show formatted SQL with syntax highlighting
        sql_query = query.sql(pretty=True, dialect=self.processor.dialect)
        st.code(sql_query, language="sql")

        # Add query details
        with st.expander("📋 Query Details"):
            st.write("**Dialect**: BigQuery")
            st.write(f"**Query Length**: {len(sql_query)} characters")
            st.write("**Purpose**: Find rows where selected columns have different values between the two tables")

            # Show estimated complexity
            if "JOIN" in sql_query.upper():
                st.write("**Complexity**: JOIN operation - may take longer for large datasets")
            if "FULL OUTER JOIN" in sql_query.upper():
                st.write("**Note**: Using FULL OUTER JOIN to capture all differences")

        st.divider()


def render_app_with_components() -> None:
    """Render the complete app using modular components."""
    # Initialize session state
    _initialize_session_state()

    # Table selection
    with st.form(key="first_step"):
        table_form = TableSelectionForm()
        table_form.render()

    if not st.session_state.get("config_tables", False):
        return

    # Get processor
    processor = _get_processor()

    # Column configuration
    with st.form(key="second_step"):
        column_form = ColumnConfigurationForm(processor)
        column_form.render()

    if not st.session_state.get("loaded_tables", False):
        return

    # Configure processor
    processor.set_config_data(
        primary_key=st.session_state.primary_key,
        columns_to_compare=st.session_state.columns_to_compare,
        sampling_rate=st.session_state.sampling_rate,
    )

    # Primary key analysis
    pk_analysis = PrimaryKeyAnalysisComponent(processor)
    if not pk_analysis.render():
        return

    # Column difference analysis
    column_analysis = ColumnDifferenceAnalysisComponent(processor)
    selected_columns = column_analysis.render()

    # Row difference viewer
    if selected_columns:
        row_viewer = RowDifferenceViewerComponent(processor)
        row_viewer.render(selected_columns)


def _initialize_session_state() -> None:
    """Initialize session state from query parameters."""
    # Set defaults if not present
    defaults = {
        "config_tables": False,
        "loaded_tables": False,
        "table1": """SELECT user_id, account_aao_automation_rate_28, account_aao_automation_rate_28_round
        FROM `gorgias-growth-production.dbt_activation.act_candu_ai_user_traits`
        """,
        "table2": """SELECT user_id, account_aao_automation_rate_28, account_aao_automation_rate_28_round
        FROM `gorgias-growth-production.dbt_activation.act_user_traits`
        """,
        "sampling_rate": 100,
        "primary_key": ["user_id"],
        "columns_to_compare": None,
        "is_select_all": False,
    }

    for key, default_value in defaults.items():
        if key not in st.session_state:
            query_value = st.query_params.get(key, default_value)
            if key == "sampling_rate":
                st.session_state[key] = int(query_value)
            elif key == "primary_key":
                st.session_state[key] = query_value.split(",") if isinstance(query_value, str) else query_value
            elif key == "columns_to_compare":
                st.session_state[key] = query_value.split(",") if query_value else None
            elif key == "is_select_all":
                st.session_state[key] = str(query_value).lower() == "true"
            else:
                st.session_state[key] = query_value


@st.cache_resource
def _get_processor() -> BigQueryProcessor:
    """Get BigQuery processor with caching."""
    return BigQueryProcessor(
        query1=st.session_state.table1,
        query2=st.session_state.table2,
    )