import streamlit as st
from streamlit_tags import st_tags
import pandas as pd
from data_formatter import highlight_diff_dataset, style_gradient, style_percentage
from processors.bigquery import BigQueryProcessor

class DataDiff:
    def __init__(self) -> None:
        self.df1: pd.DataFrame = None
        self.df2: pd.DataFrame = None

        self.primary_key: str = None
        self.columns_to_compare: str = None

        self.processor: BigQueryProcessor = None

        st.set_page_config(layout="wide")
        st.title("data-check 🔍")

        self.init_from_query_params()

    @staticmethod
    def display_results(results: list) -> None:
        st.table(results)

    @staticmethod
    @st.cache_data(show_spinner=False)
    def split_frame(input_df, rows) -> pd.DataFrame:
        df = [input_df.loc[i : i + rows - 1, :] for i in range(0, len(input_df), rows)]
        return df

    def set_session_state_from_query_params(self, key: str, default_value: str, cast_as: str = None) -> str:
        if key not in st.session_state:
            value_to_set = st.experimental_get_query_params().get(key, [default_value])[0]
            if cast_as == "int":
                st.session_state[key] = int(value_to_set)
            elif cast_as == "list":
                st.session_state[key] = value_to_set.split(",") if value_to_set else []
            elif cast_as == "bool":
                st.session_state[key] = str(value_to_set).lower() == "true"
            else:
                st.session_state[key] = value_to_set

    def init_from_query_params(self):
        self.set_session_state_from_query_params("use_sql", "False", cast_as="bool")
        self.set_session_state_from_query_params("table1", "gorgias-growth-production.dbt_activation.act_candu_ai_user_traits")
        self.set_session_state_from_query_params("table2", "gorgias-growth-development.dbt_development_antoineballiet.act_candu_ai_user_traits")
        self.set_session_state_from_query_params("sampling_rate", "100", cast_as="int")
        self.set_session_state_from_query_params("primary_key", "user_id")

        st.session_state.config_tables = st.experimental_get_query_params().get("table1", [None])[0] and st.experimental_get_query_params().get("table2", [None])[0]

        self.set_session_state_from_query_params("columns_to_compare", None, cast_as="list")
        self.set_session_state_from_query_params("is_select_all", "False")

        st.session_state.loaded_tables = st.experimental_get_query_params().get("columns_to_compare", [None])[0] or st.experimental_get_query_params().get("is_select_all", [None])[0]

    def update_first_step(self):
        st.session_state.use_sql = st.session_state.temp_use_sql
        st.session_state.table1 = st.session_state.temp_table_1
        st.session_state.table2 = st.session_state.temp_table_2
        st.session_state.sampling_rate = st.session_state.temp_sampling_rate

        st.experimental_set_query_params(
            sampling_rate=st.session_state.sampling_rate,
            table1=st.session_state.table1,
            table2=st.session_state.table2,
            use_sql=str(st.session_state.use_sql).lower(),
        )

        st.session_state.config_tables = True
        st.session_state.loaded_tables = False

    def get_processor(self) -> BigQueryProcessor:
        return BigQueryProcessor(query1=st.session_state.table1, query2=st.session_state.table2, use_sql=st.session_state.use_sql, sampling_rate=st.session_state.sampling_rate)

    def first_step(self):
        """First step of the app: select tables and sampling rate"""

        st.toggle(
            "Use SQL",
            value=st.session_state["use_sql"],
            key="temp_use_sql",
        )

        st.text_area(
            "Table 1",
            value=st.session_state["table1"],
            key="temp_table_1",
        )

        st.text_area(
            "Table 2",
            value=st.session_state["table2"],
            key="temp_table_2",
        )

        st.slider(
            "Data sampling",
            min_value=10,
            max_value=100,
            step=1,
            key="temp_sampling_rate",
            value=st.session_state["sampling_rate"],
        )

        st.form_submit_button(label="OK", on_click=self.update_first_step)

    def update_second_step(self):
        st.session_state.primary_key = st.session_state.temp_primary_key
        st.session_state.is_select_all = st.session_state.temp_is_select_all

        if st.session_state.is_select_all:
            st.session_state.columns_to_compare = (
                st.session_state.common_table_schema.columns_names
            )
        else:
            st.session_state.columns_to_compare = (
                st.session_state.temp_columns_to_compare
            )

        st.experimental_set_query_params(
            sampling_rate=st.session_state.sampling_rate,
            table1=st.session_state.table1,
            table2=st.session_state.table2,
            primary_key=st.session_state.primary_key,
            columns_to_compare=",".join(st.session_state.columns_to_compare),
            select_all=st.session_state.is_select_all,
        )

        st.session_state.loaded_tables = True

    def second_step(self):
        """Second step of the app: select primary key and columns to compare"""
        processor = self.get_processor()

        if not processor.use_sql:
            st.write("Retrieving list of common columns...")

            common_table_schema = processor.get_common_schema_from_tables(
                st.session_state.table1, st.session_state.table2
            )
            st.session_state.common_table_schema = common_table_schema

            taschema_table_1, schema_table_2 = processor.client.get_tables_schemas(table1=st.session_state.table1, table2=st.session_state.table2)
            diff_columns1, diff_columns2 = processor.get_diff_columns(taschema_table_1, schema_table_2)
            st.write("Columns exclusive to table 1 :")
            st.dataframe(diff_columns1, width=1400)
            st.write("Columns exclusive to table 2 :")
            st.dataframe(diff_columns2, width=1400)

            primary_key_select_index = common_table_schema.columns_names.index(st.session_state.primary_key) if st.session_state.primary_key in common_table_schema.columns_names else None

            st.selectbox(
                "Select primary key:",
                common_table_schema.columns_names,
                key="temp_primary_key",
                index=primary_key_select_index
            )

            st.multiselect(
                "Select columns to compare:",
                common_table_schema.columns_names,
                key="temp_columns_to_compare",
                default=st.session_state.columns_to_compare,
            )

            st.checkbox(
                "Select all",
                key="temp_is_select_all",
                value=str(st.session_state.is_select_all).lower() == "true",
            )

            st.form_submit_button(label="OK", on_click=self.update_second_step)

    def window(self):
        # Parse query params from URL
        self.init_from_query_params()

        with st.form(key="first_step"):
            self.first_step()

        if st.session_state.config_tables:
            with st.form(key="second_step"):
                self.second_step()

        if st.session_state.loaded_tables:
            # Using BigQueryClient to run queries, output primary keys in common and exclusive to each table on streamlit : display rows in table format
            st.write("Analyzing primary keys...")
            results_primary_keys = run_query_compare_primary_keys(
                st.session_state.table1,
                st.session_state.table2,
                st.session_state.primary_key,
            )
            st.dataframe(style_percentage(results_primary_keys, columns=["missing_primary_keys_ratio"]))

            if results_primary_keys["missing_primary_keys_ratio"].iloc[0] > 0 and st.button("Display exclusive primary keys for each table"):
                st.write("Displaying rows where primary keys are different...")
                df_exlusive_table1, df_exlusive_table2 = run_query_exclusive_primary_keys(table1=st.session_state.table1, table2=st.session_state.table2, primary_key=st.session_state.primary_key)

                st.write("Exclusive to table 1 :")
                st.dataframe(df_exlusive_table1)

                st.write("Exclusive to table 2 :")
                st.dataframe(df_exlusive_table2)

            st.write("Computing difference ratio...")
            results_ratio_per_column = get_column_diff_ratios(
                table1=st.session_state.table1,
                table2=st.session_state.table2,
                primary_key=st.session_state.primary_key,
                selected_columns=st.session_state.columns_to_compare,
                common_table_schema=st.session_state.common_table_schema,
                sampling_rate=st.session_state.sampling_rate,
            )

            origin_columns = results_ratio_per_column.columns

            results_ratio_per_column.insert(0, "Select", False)
            df_with_selections = style_percentage(
                results_ratio_per_column, columns=["percentage_diff_values", "ratio_not_null", "ratio_not_equal"]
            )
            df_with_selections = style_gradient(
                df_with_selections, columns=["percentage_diff_values"]
            )
            df_with_selections = style_gradient(
                df_with_selections,
                columns=["ratio_not_null"],
                gradient_color="white,blue",
            )

            # Get dataframe row-selections from user with st.data_editor
            edited_df = st.data_editor(
                data=df_with_selections,
                hide_index=True,
                column_config={
                    "Select": st.column_config.CheckboxColumn(required=True)
                },
                disabled=origin_columns,
            )

            df_selection = edited_df[edited_df.Select]

            if not df_selection.empty:
                columns_to_display = df_selection.column.tolist()

                st.write(f"Displaying rows where {columns_to_display} is different...")

                dataset = get_plain_diff(
                    table1=st.session_state.table1,
                    table2=st.session_state.table2,
                    primary_key=st.session_state.primary_key,
                    selected_columns=columns_to_display,
                    common_table_schema=st.session_state.common_table_schema,
                    sampling_rate=st.session_state.sampling_rate,
                )

                top_menu = st.columns(3)
                with top_menu[0]:
                    sort = st.radio(
                        "Sort Data", options=["Yes", "No"], horizontal=1, index=1
                    )
                if sort == "Yes":
                    with top_menu[1]:
                        sort_field = st.selectbox("Sort By", options=dataset.columns)
                    with top_menu[2]:
                        sort_direction = st.radio(
                            "Direction", options=["⬆️", "⬇️"], horizontal=True
                        )
                    dataset = dataset.sort_values(
                        by=sort_field,
                        ascending=sort_direction == "⬆️",
                        ignore_index=True,
                    )
                pagination = st.container()

                bottom_menu = st.columns((4, 1, 1))
                with bottom_menu[2]:
                    batch_size = st.selectbox("Page Size", options=[25, 50, 100, 500])
                with bottom_menu[1]:
                    total_pages = (
                        int(len(dataset) / batch_size)
                        if int(len(dataset) / batch_size) > 0
                        else 1
                    )
                    current_page = st.number_input(
                        "Page", min_value=1, max_value=total_pages, step=1
                    )
                with bottom_menu[0]:
                    st.markdown(f"Page **{current_page}** of **{total_pages}** ")

                pages = self.split_frame(dataset, batch_size)
                # pagination.dataframe(data=pages[current_page - 1], use_container_width=True)

                pagination.dataframe(
                    data=highlight_diff_dataset(
                        pages[current_page - 1], columns=columns_to_display
                    ),
                    use_container_width=True,
                )


if __name__ == "__main__":
    dd = DataDiff()
    dd.window()
