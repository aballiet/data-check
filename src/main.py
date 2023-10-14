import streamlit as st
import pandas as pd
from typing import List, Tuple
from client import get_columns, query_table
from data_processor import ComputeDiff, get_common_columns
from data_formatter import highlight_selected_text
from tools import run_multithreaded

st.set_page_config(layout="wide")
st.title('data-diff homemade 🏠')

if 'config_tables' not in st.session_state:
    st.session_state.config_tables = False

if 'loaded_tables' not in st.session_state:
    st.session_state.loaded_tables = False

if 'loaded_dataframes' not in st.session_state:
    st.session_state.loaded_dataframes = False

df1 = None
df2 = None

primary_key = None
columns_to_compare = None

def get_table_columns(table1:str, table2:str) -> Tuple[List[str], List[str]]:
    jobs = [(get_columns, {"table": table1}), (get_columns, {"table": table2})]
    columns_table_1, columns_table_2 = run_multithreaded(jobs=jobs, max_workers=2)
    return columns_table_1, columns_table_2

def get_dataframes(table1:str, table2:str, columns: list[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    jobs = [(query_table, {'table': table1, 'columns': columns}), (query_table, {'table': table2, 'columns': columns})]
    df1, df2 = run_multithreaded(jobs=jobs, max_workers=2)
    return df1, df2

def update_first_step():
    st.session_state.table1 = st.session_state.temp_table_1
    st.session_state.table2 = st.session_state.temp_table_2
    st.session_state.config_tables = True

with st.form(key='first_step'):
    st.text_input('Enter first table name:', value="gorgias-growth-production.dbt_activation.act_candu_ai_user_traits", key='temp_table_1')
    st.text_input('Enter second table name:', value="gorgias-growth-development.dbt_development_antoineballiet.act_candu_ai_user_traits", key='temp_table_2')
    submit = st.form_submit_button(label='OK', on_click=update_first_step)

def update_second_step():
    st.session_state.primary_key = st.session_state.temp_primary_key
    st.session_state.columns_to_compare = st.session_state.temp_columns_to_compare
    st.session_state.loaded_tables = True

if st.session_state.config_tables:
    with st.form(key='second_step'):
        st.write('Retrieving list of common columns...')
        columns_table_1, columns_table_2 = get_table_columns(st.session_state.table1, st.session_state.table2)
        common_columns = get_common_columns(columns_table_1, columns_table_2)
        st.selectbox('Select primary key:', list(common_columns), key='temp_primary_key')
        st.multiselect('Select columns to compare:', list(common_columns), key='temp_columns_to_compare')
        submit = st.form_submit_button(label='OK', on_click=update_second_step)

# Given a list of results returned by bigquery, display them in a table format
def display_results(results: list) -> None:
    st.table(results)

if st.session_state.loaded_tables:
    if st.button('Compare Data'):

        # Using BigQueryClient to run queries, output primary keys in common and exclusive to each table on streamlit : display rows in table format
        # results = client.run_query(compare_tables_primary_key_query(st.session_state.table1, st.session_state.table2, st.session_state.primary_key))
        # st.write(display_results(results))

        # Create dataframes from the BigQuery results
        st.write('Creating dataframes...')
        df1, df2 = get_dataframes(table1=st.session_state.table1, table2=st.session_state.table2, columns=st.session_state.columns_to_compare + [st.session_state.primary_key])

        diff = ComputeDiff(
            table1=st.session_state.table1,
            table2=st.session_state.table2,
            df1=df1,
            df2=df2,
            primary_key=st.session_state.primary_key
        )

        st.session_state.loaded_dataframes = True

        # Compute the difference ratio between the two dataframes
        st.write('Computing difference ratio...')

        df_diff = diff.format_common_value_ratios()
        st.write(display_results(df_diff))

if st.session_state.loaded_dataframes:
    with st.form(key='third_step'):
        # st.selectbox('Select column to display full-diff:', list(st.session_state.columns_to_compare), key='temp_column_to_display')
        # st.dataframe(diff.display_diff_rows(st.session_state.temp_column_to_display))

        df = pd.DataFrame({'item_name': ['Chocolate is the best', 'We love Chocolate',
                                        'I would pay money for Chocolate', 'Biscuit',
                                        'Biscuit', 'Biscuit',
                                        'IceCream', 'Dont love IceCream',
                                        'IceCream'],
                          'value': [90, 50, 86, 87, 42, 48,
                                    68, 92, 102],
                          'weight': [4, 2, 3, 5, 6, 5, 3, 7,
                                     5]})

        df["highlighted"] = df.apply(highlight_selected_text, axis=1)
        st.markdown(df.to_html(escape=False),unsafe_allow_html=True)