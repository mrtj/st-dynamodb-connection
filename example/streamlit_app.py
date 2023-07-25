import streamlit as st

from dynamodb_connection import DynamoDBConnection

"# Streamlit DynamoDB Connection"

"""
A simple demo for Streamlit DynamoDB Connection.
"""

"## DynamoDB connection"
with st.echo():
    conn = st.experimental_connection("dynamodb", type=DynamoDBConnection)

"### Get all items in the table"
with st.echo():
    df = conn.scan()
    df

"### Get a single item by its key"
with st.echo():
    article1 = conn.get_item("article1")
    article1

from dynamodb_connection.dynamodb_connection import DynamoDBMapping
tbl = DynamoDBMapping(**st.secrets["connections"]["dynamodb"])

for k,v in tbl.items():
    (k, v)
