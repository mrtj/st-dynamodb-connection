from typing import List
from collections.abc import Mapping, Sequence
import json

import streamlit as st

from dynamodb_connection import DynamoDBConnection

def update_from_editor_state(editor_state):
    pass

def get_json_serializable_cols(df) -> List[str]:
    res = []
    _, row = next(df.iterrows())
    for label, value in row.items():
        if isinstance(value, (Mapping, Sequence)) and not isinstance(value, (str, bytes, bytearray)):
            res.append(label)
    return res


def editable_demo():
    with st.echo():
        conn = st.experimental_connection("dynamodb", type=DynamoDBConnection)
        df = conn.items()
        json_cols = get_json_serializable_cols(df)
        for json_col in json_cols:
            df[json_col] = df[json_col].apply(lambda o: json.dumps(o))
        edited_df = st.data_editor(df, num_rows="dynamic", key="data_editor")
        for json_col in json_cols:
            try:
                edited_df[json_col] = edited_df[json_col].apply(lambda s: json.loads(s) if s is not None else None)
            except json.JSONDecodeError:
                st.error(f"Invalid json string in column '{json_col}'")
                st.stop()
        st.dataframe(edited_df)
        st.write(st.session_state.data_editor)

editable_demo()
