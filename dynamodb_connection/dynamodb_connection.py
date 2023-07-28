"""Streamlit connection to an Amazon DynamoDB table."""

from typing import Any,  Dict, Iterator, Iterable, Union, Literal, Optional, cast

from streamlit.connections import ExperimentalBaseConnection

import boto3
import pandas as pd

from .dynamodb_mapping import DynamoDBMapping, DynamoDBKeySimplified, DynamoDBItemType
from .utils import boto3_session_from_config

DynamoDBConnectionApiType = Literal["raw", "pandas"]

class DynamoDBConnection(ExperimentalBaseConnection[DynamoDBMapping]):
    """Connects a streamlit app to an Amazon DynamoDB table.

    Example usage:

    ```python
    import streamlit as st
    from dynamodb_connection import DynamoDBConnection
    conn = st.experimental_connection("dynamodb", type=DynamoDBConnection)
    df = conn.items() # table contents returned as pandas dataframe
    ```

    Args (can be passed to `st.experimental_connection`):
        connection_name (str): the name of the connection
        api_type (DynamoDBConnectionApiType): the API type to be used. If set to "pandas", pandas
            dataframe or series will be returned by the methods. If set to "raw", raw python objects
            (lists, dictionaries) will be returned. Defaults to "pandas".

    Additionally, the following optional keyword arguments can be used to configure the underlying
    boto3 client:

        - table_name (str): The name of the DynamoDB table
        - boto3_session (boto3.Session): a preconfigured boto3 session
        - aws_access_key_id (str): AWS Access Key ID
        - aws_secret_access_key (str): AWS Secret Access Key
        - aws_region (str): AWS region name
        - aws_profile (str): AWS profile name
    """
    def __init__(self,
        connection_name: str, api_type: DynamoDBConnectionApiType="pandas", **kwargs
    ) -> None:
        self.api_type = api_type
        super().__init__(connection_name, **kwargs)

    @property
    def mapping(self) -> DynamoDBMapping:
        """Access the underlying DynamoDBMapping for full API operations."""
        return self._instance

    def _connect(self, **kwargs) -> DynamoDBMapping:
        secrets = self._secrets.to_dict()
        table_name = kwargs.get("table_name") or secrets.get("table_name")
        if table_name is None:
            raise ValueError(
                "You must configure the DynamoDB table name either as a DynamoDBConnection secret "
                "called 'table_name' or pass it as keyword parameter 'table_ name' when creating "
                "the connection."
            )

        session = (
            kwargs.get("boto3_session") or
            boto3_session_from_config(kwargs) or
            boto3_session_from_config(secrets) or
            boto3.Session()
        )
        table = DynamoDBMapping(table_name=table_name, boto3_session=session)
        return table

    def _df_from_iterable(self, iterable: Iterable[DynamoDBItemType]) -> pd.DataFrame:
        df = pd.DataFrame(cast(Iterable[Dict[Any, Any]], iterable))
        df.set_index(list(self.mapping.key_names), inplace=True)
        return df

    def items(self,
        api_type: Optional[DynamoDBConnectionApiType]=None, **kwargs
    ) -> Union[Iterator[DynamoDBItemType], pd.DataFrame]:
        """Returns all items in the DynamoDB table.

        The items can be returned either as a pandas dataframe (the default behavior) or as an
        iterator of python dictionaries. If you have a huge table, it is highly recommended to
        use the the dict iterator mode, otherwise DynamoDBConnection will try to read the whole
        table into the memory. The iterator mode scans the successive pages of your table on-demand.

        Args:
            api_type (Optional[DynamoDBConnectionApiType]): Specifies the return type of the
                method: "pandas", "raw" or None. The default None will use the connection's api
                configuration.
            **kwargs: Optional keyword arguments to be passed to the underlying boto3 DynamoDB
                scan operation.

        Return (Union[Iterator[DynamoDBItemType], pd.DataFrame]): Either a pandas dataframe, or an
            iterator of python dictionaries.
        """
        api_type = api_type or self.api_type
        iterator = self.mapping.scan(**kwargs)
        return iterator if api_type == "raw" else self._df_from_iterable(iterator)

    def get_item(self,
        keys: DynamoDBKeySimplified,
        api_type: Optional[DynamoDBConnectionApiType]=None,
        **kwargs
    ) -> Union[DynamoDBItemType, pd.Series]:
        """Retrieves a single item from the table.

        The value(s) of the item's key(s) should be specified.

        Args:
            keys (DynamoDBKeySimplified): The key value. This can either be a simple Python type,
                if only the partition key is specified in the table's key schema, or a tuple of the
                partition key and the range key values, if both are specified in the key schema.
            api_type (Optional[DynamoDBConnectionApiType]): Specifies the return type of the
                method: "pandas", "raw" or None. The default None will use the connection's api
                configuration.
            **kwargs: keyword arguments to be passed to the underlying DynamoDB get_item operation.

        Raises:
            ValueError: If the required key values are not specified.
            KeyError: If no item can be found under this key in the table.

        Returns:
            Union[DynamoDBItemType, pd.Series]: Either a mapping or a pandas series, containing
                the data of the requested item.
        """
        api_type = api_type or self.api_type
        item = self.mapping.get_item(keys, **kwargs)
        return item if api_type == "raw" else pd.Series(item, name="value")

    def set_item(self,
        keys: DynamoDBKeySimplified, item: Union[DynamoDBItemType, pd.Series], **kwargs
    ) -> None:
        """Creates or overwrites a single item in the table.

        Args:
            keys (DynamoDBKeySimplified): The key value. This can either be a simple Python type,
                if only the partition key is specified in the table's key schema, or a tuple of the
                partition key and the range key values, if both are specified in the key schema.
            item (Union[DynamoDBItemType, pd.Series]): Either a mapping or a pandas Series
                containing only object types supported by DynamoDB.
            **kwargs: keyword arguments to be passed to the underlying DynamoDB set_item operation.
        """
        item_ = dict(item) if isinstance(item, pd.Series) else item
        self.mapping.set_item(keys, cast(DynamoDBItemType, item_), **kwargs)

    def put_item(self, keys: DynamoDBKeySimplified, item: DynamoDBItemType, **kwargs) -> None:
        """An alias of the `set_item` method."""
        self.set_item(keys, item, **kwargs)

    def modify_item(self,
        keys: DynamoDBKeySimplified, modifications: Union[DynamoDBItemType, pd.Series], **kwargs
    ) -> None:
        """Modifies an existing item in the table.

        Args:
            keys (DynamoDBKeySimplified): The key value. This can either be a simple Python type,
                if only the partition key is specified in the table's key schema, or a tuple of the
                partition key and the range key values, if both are specified in the key schema.
            modifications (Union[DynamoDBItemType, pd.Series]): Either a mapping or a pandas Series
                containing containing the desired modifications to the fields of the item. This
                mapping follows the same format as the entire item, but it isn't required to contain
                all fields: fields that are omitted will be unaffected. To delete a field, set the
                field value to None.
            **kwargs: keyword arguments to be passed to the underlying DynamoDB update_item
                operation.
        """
        mods_ = dict(modifications) if isinstance(modifications, pd.Series) else modifications
        self.mapping.modify_item(keys, cast(DynamoDBItemType, mods_), **kwargs)

    def del_item(self, keys: DynamoDBKeySimplified, **kwargs) -> None:
        """Deletes a single item from the table.

        Args:
            keys (DynamoDBKeySimplified): The key value. This can either be a simple Python type,
                if only the partition key is specified in the table's key schema, or a tuple of the
                partition key and the range key values, if both are specified in the key schema.
        """
        self.mapping.del_item(keys, **kwargs)
