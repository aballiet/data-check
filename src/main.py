import streamlit as st
import pandas as pd
from client import BigQueryClient
from data_processor import compare_tables_primary_key_query, compute_common_value_ratios
from time import sleep

client = BigQueryClient()

st.title('Perform Data Check on BigQuery Tables')

if 'config_tables' not in st.session_state:
    st.session_state.config_tables = False

if 'loaded_tables' not in st.session_state:
    st.session_state.loaded_tables = False

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
        common_columns = client.get_common_columns(st.session_state.table1, st.session_state.table2)
        st.selectbox('Select primary key:', list(common_columns), key='temp_primary_key')
        st.multiselect('Select columns to compare:', list(common_columns), key='temp_columns_to_compare')
        submit = st.form_submit_button(label='OK', on_click=update_second_step)

# Given a list of results returned by bigquery, display them in a table format
def display_results(results: list) -> None:
    st.table(results)

if st.session_state.loaded_tables:
    if st.button('Compare Data'):
        st.write('Comparing data...')
        # Using BigQueryClient to run queries, output primary keys in common and exclusive to each table on streamlit : display rows in table format
        results = client.run_query(compare_tables_primary_key_query(st.session_state.table1, st.session_state.table2, st.session_state.primary_key))
        st.write(display_results(results))

        # Create dataframes from the BigQuery results
        st.write('Creating dataframes...')
        df1 = client.query_table(st.session_state.table1, st.session_state.columns_to_compare)
        df2 = client.query_table(st.session_state.table2, st.session_state.columns_to_compare)

        # Compute the difference ratio between the two dataframes
        st.write('Computing difference ratio...')
        df_diff = compute_common_value_ratios(df1, df2, primary_key=st.session_state.primary_key)
        st.write(display_results(df_diff))