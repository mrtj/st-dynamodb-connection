from collections.abc import Iterator

import streamlit as st
import pandas as pd

from dynamodb_connection import DynamoDBConnection, DynamoDBConnectionApiType

def show_result(result):
    if isinstance(result, pd.DataFrame):
        st.write(result)
    elif isinstance(result, Iterator):
        for item in result:
            st.write(item)
    else:
        st.write(result)

CREATE_CONN_CODE = """\
conn = st.experimental_connection(
    "dynamodb", type=DynamoDBConnection, api_type="{api_type}"
)"""

RAW_GET_ITEM_CODE = """\
for item in conn.items():
    st.write(item)\
"""

PANDAS_GET_ITEM_CODE = """st.write(conn.items())"""

def api_demo(api_type: DynamoDBConnectionApiType):

    f"### Create DynamoDB connection with {api_type} api"
    st.code(CREATE_CONN_CODE.format(api_type=api_type))
    conn = st.experimental_connection(
        "dynamodb", type=DynamoDBConnection, api_type=api_type
    )

    "### Get all items in the table"
    if api_type == "raw":
        st.code(RAW_GET_ITEM_CODE)
        for item in conn.items():
            st.write(item)
    elif api_type == "pandas":
        st.code(PANDAS_GET_ITEM_CODE)
        st.write(conn.items())

    "### Get a single item by key"
    with st.echo():
        article1 = conn.get_item("article1")
        article1

    "### Put an item in the table"
    with st.echo():
        conn.put_item("article_st", {
            "text": "This item was put from streamlit!",
            "metadata": {"source": "mrtj"},
        })
        show_result(conn.items())

    "### Delete an item from the table"
    with st.echo():
        conn.del_item("article_st")
        show_result(conn.items())

def dict_demo():
    "### Create DynamoDB connection"
    with st.echo():
        conn = st.experimental_connection("dynamodb", type=DynamoDBConnection)

    "### Get all items in the table"
    with st.echo():
        def show_items(items):
            for key, value in items:
                st.write(key, value)
        show_items(conn.table.items())

    "### Get a single item by key"
    with st.echo():
        article1 = conn.table["article1"]
        article1

    "### Put an item in the table"
    with st.echo():
        conn.table["article_st"] = {
            "text": "This item was put from streamlit!",
            "metadata": {"source": "mrtj"},
        }
        show_items(conn.table.items())

    "### Delete an item from the table"
    with st.echo():
        del conn.table["article_st"]
        show_items(conn.table.items())

"""# Streamlit DynamoDB Connection

A simple demo for Streamlit DynamoDB Connection.
"""

pandas_tab, raw_tab, dict_tab = st.tabs(("Pandas API", "Raw API", "Underlying dictionary API"))

with pandas_tab:
    api_demo("pandas")

with raw_tab:
    api_demo("raw")

with dict_tab:
    dict_demo()
