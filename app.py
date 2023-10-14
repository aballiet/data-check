import streamlit as st
from client import BigQueryClient
from diff import compare_tables_primary_key_query, compare_exclusive_tables_primary_key_query

table1 = st.text_input('Enter first table name:', value="gorgias-growth-production.dbt_activation.act_candu_ai_user_traits")
table2 = st.text_input('Enter second table name:', value="gorgias-growth-development.dbt_development_antoineballiet.act_candu_ai_user_traits")

# User should be able to specify primary key to use for comparison
primary_key = st.text_input('Enter primary key name:', value="user_id")

client = BigQueryClient()

# Given a list of results returned by bigquery, display them in a table format
def display_results(results: list) -> None:
    st.table(results)


if st.button('Compare tables'):

    columns_table_1 = client.get_columns(table1)
    columns_table_2 = client.get_columns(table2)

    # Using BigQueryClient to run queries, output primary keys in common and exclusive to each table on streamlit : display rows in table format
    results = client.run_query(compare_tables_primary_key_query(table1, table2, primary_key))
    st.write(f"Primary keys in common: {primary_key}")
    st.write(f"Primary keys exclusive to {table1}: {primary_key}")
    st.write(f"Primary keys exclusive to {table2}: {primary_key}")
    st.write(display_results(results))

    # Compute the number of rows that are different between the two tables and details per column the percentage of diff in a table format, display percentage of different values per column on top of the table
    results = client.run_query(compare_exclusive_tables_primary_key_query(table1, table2, primary_key))
    st.write(f"Number of rows that are different between the two tables: {primary_key}")
    st.write(display_results(results))