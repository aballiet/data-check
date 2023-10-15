import streamlit as st
import pandas as pd
from data_processor import ComputeDiff
from data_helpers import get_table_schemas, run_query_compare_primary_keys, get_column_diff_ratios, get_common_schema, get_plain_diff

class DataDiff():
    def __init__(self) -> None:
        self.df1: pd.DataFrame = None
        self.df2: pd.DataFrame = None
        self.diff: ComputeDiff = None

        self.primary_key: str = None
        self.columns_to_compare: str = None

        st.set_page_config(layout="wide")
        st.title('data-diff homemade 🏠')

        if 'config_tables' not in st.session_state:
            st.session_state.config_tables = False

        if 'loaded_tables' not in st.session_state:
            st.session_state.loaded_tables = False

        if 'display_diff' not in st.session_state:
            st.session_state.display_diff = False

        if 'column_to_display' not in st.session_state:
            st.session_state.column_to_display = None

    @staticmethod
    def display_results(results: list) -> None:
        st.table(results)

    @staticmethod
    @st.cache_data(show_spinner=False)
    def split_frame(input_df, rows):
        df = [input_df.loc[i : i + rows - 1, :] for i in range(0, len(input_df), rows)]
        return df

    def update_first_step(self):
        st.session_state.table1 = st.session_state.temp_table_1
        st.session_state.table2 = st.session_state.temp_table_2
        st.session_state.sampling_rate = st.session_state.temp_sampling_rate
        st.session_state.config_tables = True
        st.session_state.loaded_tables = False

    def update_second_step(self):
        st.session_state.primary_key = st.session_state.temp_primary_key
        st.session_state.is_select_all = st.session_state.temp_is_select_all

        if st.session_state.is_select_all:
            st.session_state.columns_to_compare = st.session_state.common_table_schema.columns_names
        else:
            st.session_state.columns_to_compare = st.session_state.temp_columns_to_compare

        st.session_state.loaded_tables = True

    def update_third_step(self):
        st.session_state.display_diff = True

    def window(self):
        with st.form(key='first_step'):
            st.text_input('Table 1', value="gorgias-growth-production.dbt_activation.act_candu_ai_user_traits", key='temp_table_1')
            st.text_input('Table 2', value="gorgias-growth-development.dbt_development_antoineballiet.act_candu_ai_user_traits", key='temp_table_2')
            st.slider("Data sampling", min_value=10, max_value=100, step=1, key="temp_sampling_rate")
            submit = st.form_submit_button(label='OK', on_click=self.update_first_step)

        if st.session_state.config_tables:
            with st.form(key='second_step'):
                st.write('Retrieving list of common columns...')
                schema_table_1, schema_table_2 = get_table_schemas(st.session_state.table1, st.session_state.table2)
                st.session_state.schema_table_1 = schema_table_1
                st.session_state.schema_table_2 = schema_table_2

                common_table_schema = get_common_schema(st.session_state.table1, st.session_state.table2)
                st.session_state.common_table_schema = common_table_schema

                st.selectbox('Select primary key:', common_table_schema.columns_names, key='temp_primary_key')

                st.multiselect('Select columns to compare:', common_table_schema.columns_names, key='temp_columns_to_compare')
                st.checkbox("Select all", key="temp_is_select_all")

                submit = st.form_submit_button(label='OK', on_click=self.update_second_step)

        if st.session_state.loaded_tables:
            with st.form(key='third_step'):
                # Using BigQueryClient to run queries, output primary keys in common and exclusive to each table on streamlit : display rows in table format
                st.write('Analyzing primary keys...')
                results_primary_keys = run_query_compare_primary_keys(st.session_state.table1, st.session_state.table2, st.session_state.primary_key)
                st.dataframe(results_primary_keys)

                st.write('Computing difference ratio...')
                results_ratio_per_column = get_column_diff_ratios(table1=st.session_state.table1, table2=st.session_state.table2, primary_key=st.session_state.primary_key, selected_columns=st.session_state.columns_to_compare, common_table_schema=st.session_state.common_table_schema, sampling_rate=st.session_state.sampling_rate)
                st.dataframe(results_ratio_per_column)

                st.selectbox('Select column to display full-diff:', st.session_state.columns_to_compare, key='column_to_display')
                button_check = st.form_submit_button(label='Show diff row-wise', on_click=self.update_third_step)

        if st.session_state.display_diff and st.session_state.column_to_display:
            st.write(f"Displaying rows where {st.session_state.column_to_display} is different...")

            dataset = get_plain_diff(table1=st.session_state.table1, table2=st.session_state.table2, primary_key=st.session_state.primary_key, selected_columns=[st.session_state.column_to_display], common_table_schema=st.session_state.common_table_schema, sampling_rate=st.session_state.sampling_rate)

            top_menu = st.columns(3)
            with top_menu[0]:
                sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=1, index=1)
            if sort == "Yes":
                with top_menu[1]:
                    sort_field = st.selectbox("Sort By", options=dataset.columns)
                with top_menu[2]:
                    sort_direction = st.radio(
                        "Direction", options=["⬆️", "⬇️"], horizontal=True
                    )
                dataset = dataset.sort_values(
                    by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True
                )
            pagination = st.container()

            bottom_menu = st.columns((4, 1, 1))
            with bottom_menu[2]:
                batch_size = st.selectbox("Page Size", options=[25, 50, 100])
            with bottom_menu[1]:
                total_pages = (
                    int(len(dataset) / batch_size) if int(len(dataset) / batch_size) > 0 else 1
                )
                current_page = st.number_input(
                    "Page", min_value=1, max_value=total_pages, step=1
                )
            with bottom_menu[0]:
                st.markdown(f"Page **{current_page}** of **{total_pages}** ")

            pages = self.split_frame(dataset, batch_size)
            pagination.dataframe(data=pages[current_page - 1], use_container_width=True)

if __name__ == '__main__':
    dd = DataDiff()
    dd.window()