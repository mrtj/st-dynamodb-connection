def dict_demo():
    import streamlit as st
    st.title("Dictionary API")
    st.write("This demo shows how to use the underlying dictionary-like API of DynamoDB Connection.")
    st.write("### Create DynamoDB connection")
    with st.echo():
        import streamlit as st
        from dynamodb_connection import DynamoDBConnection
        conn = st.experimental_connection("dynamodb", type=DynamoDBConnection)
        # Access the dictionary API with the mapping property:
        mapping = conn.mapping

    st.write("### Get all items in the table")
    st.write("Helper function for pretty visualization of keys and dictionaries:")
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
        show_items(mapping.items())

    st.write("### Get a single item by key")
    with st.echo():
        article1 = mapping["article1"]
        st.write(article1)

    st.write("### Put an item in the table")
    with st.echo():
        mapping["article_st"] = {
            "text": "This item was put from streamlit!",
            "metadata": {"author": "streamlit_user"},
        }
        show_items(mapping.items())

    st.write("### Modify an existing item")
    with st.echo():
        article = mapping["article_st"]
        # Modify an existing field:
        article["text"] = "This item was put and modified from streamlit!"
        # Erase a field:
        article["metadata"] = None
        # Add a new field:
        article["new_field"] = "This is a newly added field"
        show_items(mapping.items())

    st.write("### Approximate number of items in the table")
    with st.echo():
        st.write("Approximate number of items:", len(mapping))

    st.write("### Exact number of items in the table")
    st.warning("Warning: costly operation!", icon="⚠️")
    with st.echo():
        st.write("Exact number of items:", len(list(mapping.keys())))

    st.write("### Delete an item from the table")
    with st.echo():
        del mapping["article_st"]
        show_items(mapping.items())

dict_demo()
