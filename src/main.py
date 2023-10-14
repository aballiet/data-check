import streamlit as st
import pandas as pd
from client import BigQueryClient
from diff import compare_tables_primary_key_query, percentage_of_differences_per_column

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


    # Create dataframes from the BigQuery results
    df1 = client.run_query_to_dataframe(query=f"select user_id, account_aao_automation_rate_28_rank_perc_round from {table1}")
    df2 = client.run_query_to_dataframe(query=f"select user_id, account_aao_automation_rate_28_rank_perc_round from {table2}")

    # Find rows with differences
    differences = df1.ne(df2)
    # Highlight rows with at least one difference
    highlighted_rows = differences.any(axis=1)

    # Display the results in a Streamlit table with highlighted rows
    st.write("Comparison Result:")
    st.dataframe(differences, height=300, width=800, use_container_width=True)

    # Iterate through the rows and apply the red cell styling
    for index, row in differences.iterrows():
        if highlighted_rows[index]:
            st.dataframe(pd.DataFrame(['background-color: red'] * len(row), index=row.index).T)

    st.dataframe(df2, height=300, width=800, use_container_width=True)
