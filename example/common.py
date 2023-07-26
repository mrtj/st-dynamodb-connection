from collections.abc import Iterator

import streamlit as st
import pandas as pd

from dynamodb_connection import DynamoDBConnection, DynamoDBConnectionApiType

CREATE_CONN_CODE = """\
from dynamodb_connection import DynamoDBConnection
conn = st.experimental_connection(
    "dynamodb", type=DynamoDBConnection, api_type="{api_type}"
)"""

RAW_GET_ITEM_CODE = """\
for item in conn.items():
    st.write(item)\
"""

PANDAS_GET_ITEM_CODE = """st.write(conn.items())"""

def show_result(result):
    if isinstance(result, pd.DataFrame):
        st.write(result)
    elif isinstance(result, Iterator):
        for item in result:
            st.write(item)
    else:
        st.write(result)

def api_demo(api_type: DynamoDBConnectionApiType, intro: str):
    st.title(f"{api_type.capitalize()} API")
    st.write(f"This demo shows how to use the {api_type} API of DynamoDB Connection.")
    st.write(intro)
    st.write(f"### Create DynamoDB connection with {api_type} api")
    st.code(CREATE_CONN_CODE.format(api_type=api_type))
    conn = st.experimental_connection(
        "dynamodb", type=DynamoDBConnection, api_type=api_type
    )

    st.write("### Get all items in the table")
    if api_type == "raw":
        st.code(RAW_GET_ITEM_CODE)
        for item in conn.items():
            st.write(item)
    elif api_type == "pandas":
        st.code(PANDAS_GET_ITEM_CODE)
        st.write(conn.items())

    st.write("### Get a single item by key")
    with st.echo():
        article1 = conn.get_item("article1")
        st.write(article1)

    st.write("### Put an item in the table")
    with st.echo():
        conn.put_item("article_st", {
            "text": "This item was put from streamlit!",
            "metadata": {"source": "mrtj"},
        })
        show_result(conn.items())

    st.write("### Modify an existing item")
    with st.echo():
        conn.modify_item(
            "article_st",
            {
                "text": "This item was put and modified from streamlit!",
                "metadata": None,
                "new_field": "This is a newly added field"
            }
        )
        show_result(conn.items())

    st.write("### Delete an item from the table")
    with st.echo():
        conn.del_item("article_st")
        show_result(conn.items())
