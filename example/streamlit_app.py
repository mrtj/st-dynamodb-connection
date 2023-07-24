import streamlit as st

from dynamodb_connection import DynamoDBConnection

"# Streamlit DynamoDB Connection"

"""
A simple demo for Streamlit DynamoDB Connection.
"""

"### DynamoDB connection"
with st.echo():
    conn = st.experimental_connection("dynamodb", type=DynamoDBConnection)
    df = conn.scan()
    df

