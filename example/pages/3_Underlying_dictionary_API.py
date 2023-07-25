import streamlit as st

def dict_demo():
    st.title("Underlying dictionary API")
    st.write("This demo shows how to use the underlying dictionary-like API of DynamoDB Connection.")
    st.write("### Create DynamoDB connection")
    with st.echo():
        from dynamodb_connection import DynamoDBConnection
        conn = st.experimental_connection("dynamodb", type=DynamoDBConnection)

    st.write("### Get all items in the table")
    st.write("Helper function:")
    with st.echo():
        def show_items(items):
            col1, col2 = st.columns((1, 4))
            col1.write("*Key*\n\n---")
            col2.write("*Item*\n\n---")
            for key, value in items:
                col1, col2 = st.columns((1, 4))
                col1.write(key)
                col2.write(value)

    st.write("Show all items:")
    with st.echo():
        show_items(conn.mapping.items())

    st.write("### Get a single item by key")
    with st.echo():
        article1 = conn.mapping["article1"]
        st.write(article1)

    st.write("### Put an item in the table")
    with st.echo():
        conn.mapping["article_st"] = {
            "text": "This item was put from streamlit!",
            "metadata": {"author": "streamlit_user"},
        }
        show_items(conn.mapping.items())

    st.write("### Approximate number of items in the table")
    with st.echo():
        st.write("Approximate number of items:", len(conn.mapping))

    st.write("### Exact number of items in the table")
    st.warning("Warning: costly operation!", icon="⚠️")
    with st.echo():
        st.write("Exact number of items:", len(list(conn.mapping.keys())))

    st.write("### Delete an item from the table")
    with st.echo():
        del conn.mapping["article_st"]
        show_items(conn.mapping.items())

dict_demo()
