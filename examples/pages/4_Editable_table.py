
import streamlit as st

from dynamodb_connection import DynamoDBTableEditor

def editable_demo():
    st.title("Editable table")
    st.write("""\
        This page demonstrates the use of DynamoDBTableEditor, which allows you to edit live data in
        a DynamoDB table. Any modifications made in the table are immediately reflected in the
        DynamoDB table."""
    )
    st.write("### DynamoDBTableEditor usage")
    with st.echo():
        from dynamodb_connection import DynamoDBConnection
        conn = st.connection("dynamodb", type=DynamoDBConnection)
        table_editor = DynamoDBTableEditor(conn)
        table_editor.edit()

editable_demo()
