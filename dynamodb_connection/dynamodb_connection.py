from typing import Any,  Dict, Iterator, Iterable, Union, Literal, Optional, cast

from streamlit.connections import ExperimentalBaseConnection

import boto3
import pandas as pd

from .dynamodb_mapping import DynamoDBMapping, DynamoDBKeySimplified, DynamoDBItemType
from .utils import boto3_session_from_config

DynamoDBConnectionApiType = Literal["raw", "pandas"]

class DynamoDBConnection(ExperimentalBaseConnection[DynamoDBMapping]):
    """Connects a streamlit app to an Amazon DynamoDB table.


    """
    def __init__(self,
        connection_name: str, api_type: DynamoDBConnectionApiType="pandas", **kwargs
    ) -> None:
        self.api_type = api_type
        super().__init__(connection_name, **kwargs)

    @property
    def table(self) -> DynamoDBMapping:
        """Access the underlying DynamoDB table resource for full API operations."""
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
        df.set_index(list(self.table._key_names), inplace=True)
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
            return_raw (bool): Set to True to return the items as an iterator of python
                dictionaries.
            **kwargs: Optional keyword arguments to be passed to the underlying boto3 DynamoDB
                scan operation.

        Return (Union[Iterator[Dict[str, Any]], pd.DataFrame]): Either a pandas dataframe, or an
            iterator of python dictionaries.
        """
        api_type = api_type or self.api_type
        iterator = self.table.scan(**kwargs)
        return iterator if api_type == "raw" else self._df_from_iterable(iterator)

    def get_item(self,
        keys: DynamoDBKeySimplified,
        api_type: Optional[DynamoDBConnectionApiType]=None,
        **kwargs
    ) -> Union[DynamoDBItemType, pd.Series]:
        api_type = api_type or self.api_type
        item = self.table.get_item(keys, **kwargs)
        return item if api_type == "raw" else pd.Series(item, name="value")

    def set_item(self, keys: DynamoDBKeySimplified, item: DynamoDBItemType, **kwargs) -> None:
        self.table.set_item(keys, item, **kwargs)

    def put_item(self, keys: DynamoDBKeySimplified, item: DynamoDBItemType, **kwargs) -> None:
        self.table.put_item(keys, item, **kwargs)

    def del_item(self, keys: DynamoDBKeySimplified, **kwargs) -> None:
        self.table.del_item(keys, **kwargs)
