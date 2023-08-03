from typing import (
    Literal, Dict, Union, Mapping, Sequence, TypeVar, MutableMapping, List, cast
)
import json
import logging

import pandas as pd
import streamlit as st

from .connection import DynamoDBConnection, DynamoDBItemType


logger = logging.getLogger(__name__)


class JSONError(RuntimeError): ...


DFOrMapping = TypeVar("DFOrMapping", pd.DataFrame, MutableMapping)


def _get_json_serializable_cols(df) -> List[str]:
    res = []
    _, row = next(df.iterrows())
    for label, value in row.items():
        if isinstance(value, (Mapping, Sequence)) and not isinstance(value, (str, bytes, bytearray)):
            res.append(label)
    return res


def _serialize_json_cols(df: pd.DataFrame, json_cols: Sequence[str]) -> pd.DataFrame:
    for json_col in json_cols:
        df[json_col] = df[json_col].apply(lambda o: json.dumps(o))
    return df


def _deserialize_json_cols(data: DFOrMapping, json_cols: Sequence[str]) -> DFOrMapping:
    for json_col in json_cols:
        try:
            deserializer = lambda s: json.loads(s) if s is not None else None
            if isinstance(data, pd.DataFrame):
                data[json_col] = data[json_col].apply(deserializer)
            elif isinstance(data, MutableMapping):
                if json_col in data:
                    data[json_col] = deserializer(data[json_col])
        except json.JSONDecodeError as e:
            raise JSONError(f"Invalid json string in column '{json_col}'!") from e
    return data


class DynamoDBTableEditor:

    _DATA_EDITOR_WIDGET_KEY: Literal["data_editor_widget"] = "data_editor_widget"
    _DATA_EDITOR_DATA_KEY: Literal["data_editor_data"] = "data_editor_data"
    _DATA_EDITOR_PROCESSED_KEY: Literal["data_editor_processed"] = "data_editor_processed"
    _DEFAULT_EDIT_INFO: Dict[str, Union[Mapping, Sequence]] = {
        "edited_rows": {},
        "added_rows": [],
        "deleted_rows": []
    }

    def __init__(self, connection: DynamoDBConnection, key_prefix="table_editor_") -> None:
        self.connection = connection
        self.key_prefix = key_prefix
        self.data_key = self.key_prefix + self._DATA_EDITOR_DATA_KEY
        self.widget_key = self.key_prefix + self._DATA_EDITOR_WIDGET_KEY
        self.processed_edits_key = self.key_prefix + self._DATA_EDITOR_PROCESSED_KEY
        if not self.data_key in st.session_state:
            st.session_state[self.data_key] = self.connection.items(ignore_cache=True)
        self.df = st.session_state[self.data_key]

    def edit(self) -> pd.DataFrame:
        json_cols = _get_json_serializable_cols(self.df)
        df = _serialize_json_cols(self.df.copy(), json_cols)
        edited_df = st.data_editor(
            df,
            num_rows="dynamic",
            column_config={
                "_index": st.column_config.TextColumn(required=True),
            },
            key=self.widget_key
        )
        try:
            edited_df = _deserialize_json_cols(edited_df, json_cols=json_cols)
        except JSONError as e:
            st.error(str(e), icon="🛑")
            st.stop()
        self.process_edits(json_cols)
        return edited_df

    @property
    def edit_info(self) -> Dict:
        return st.session_state.get(
            self.widget_key,
            default=self._DEFAULT_EDIT_INFO.copy()
        )

    @property
    def processed_edits(self) -> Dict:
        if not self.processed_edits_key in st.session_state:
            st.session_state[self.processed_edits_key] = self._DEFAULT_EDIT_INFO.copy()
        return st.session_state[self.processed_edits_key]

    @processed_edits.setter
    def processed_edits(self, value):
        st.session_state[self.processed_edits_key] = value

    def process_edits(self, json_cols: List[str]) :
        edit_info = self.edit_info
        logger.debug("Edit info: %s", edit_info)

        # edited_rows
        processed_edited_rows = self.processed_edits["edited_rows"]
        for idx, edited_row in edit_info["edited_rows"].items():
            index_val = self.df.index[int(idx)]
            edited_row = _deserialize_json_cols(edited_row, json_cols)
            if idx in processed_edited_rows and processed_edited_rows[idx] == edited_row:
                logger.debug(
                    "Item '%s' edit '%s' was already processed, continue...", index_val, edited_row
                )
                continue
            self.connection.modify_item(index_val, cast(DynamoDBItemType, edited_row))
            processed_edited_rows[idx] = edited_row
            logger.debug("Item '%s' edit '%s' was processed.", index_val, edited_row)

        # added rows
        processed_added_rows = self.processed_edits["added_rows"]
        for added_row in edit_info["added_rows"]:
            added_row = _deserialize_json_cols(added_row, json_cols)
            keys = added_row.pop("_index")
            added_row_repr = json.dumps(added_row, sort_keys=True)
            if added_row_repr in processed_added_rows:
                logger.debug("New row '%s' was already added and up to date, continue...", keys)
                continue
            self.connection.put_item(keys, item=added_row)
            processed_added_rows.append(added_row_repr)
            logger.debug("Created or updated new row '%s': %s", keys, added_row)

        # deleted rows
        processed_deleted_rows = self.processed_edits["deleted_rows"]
        for idx in edit_info["deleted_rows"]:
            index_val = self.df.index[int(idx)]
            if index_val in processed_deleted_rows:
                logger.debug("Row '%s' was already deleted, continue...", index_val)
                continue
            self.connection.del_item(keys=index_val)
            processed_deleted_rows.append(index_val)
            logger.debug("Deleted row '%s'.", index_val)
