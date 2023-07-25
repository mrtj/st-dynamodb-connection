from collections.abc import Mapping, MutableMapping, Sequence, Iterable
from typing import Any, Mapping, Optional, Dict, Iterator, Union, List, Tuple, ValuesView, ItemsView


from streamlit.connections import ExperimentalBaseConnection

import boto3
import pandas as pd

# try:
import mypy_boto3_dynamodb
DynamoDBTable = mypy_boto3_dynamodb.service_resource.Table
# except ImportError:
#     DynamoDBTable = Any # type: ignore

def get_key_names(table: DynamoDBTable) -> Tuple:
    """Gets the key names of the DynamoDB table.

    Returns:
        List[str]: A list with the key names. The list has either one (if only the hash key
            is defined on the table) or two (if both the hash and range key is defined)
            elements.
    """
    keys: List[str] = []
    for schema_part in table.key_schema:
        if schema_part["KeyType"] == "HASH":
            keys.insert(0, schema_part["AttributeName"])
        elif schema_part["KeyType"] == "RANGE":
            keys.append(schema_part["AttributeName"])
    return tuple(keys)

def simplify_tuple_keys(tpl: tuple) -> Union[Any, Tuple]:
    return tpl[0] if len(tpl) == 1 else tpl

def create_tuple_keys(key: Union[str, Iterable]) -> Tuple:
    if isinstance(key, Iterable) and not isinstance(key, (str, bytes, bytearray)):
        return tuple(key)
    else:
        return (key,)

def boto3_session_from_config(config: Dict[str, Any]) -> Optional[boto3.Session]:
    if "aws_access_key_id" in config and "aws_secret_access_key" in config:
        return boto3.Session(
            aws_access_key_id=config["aws_access_key_id"],
            aws_secret_access_key=config["aws_secret_access_key"],
            region_name=config.get("aws_region"),
            profile_name=config.get("aws_profile")
        )
    else:
        return None


class DynamoDBValuesView(ValuesView):

    __slots__ = '_mapping'

    def __init__(self, mapping: "DynamoDBMapping") -> None:
        self._mapping = mapping

    def __contains__(self, value: object) -> bool:
        for v in self._mapping.scan():
            if v is value or v == value:
                return True
        return False

    def __iter__(self) -> Iterator:
        return self._mapping.scan()


class DynamoDBItemsView(ItemsView):

    __slots__ = '_mapping'

    def __init__(self, mapping: "DynamoDBMapping") -> None:
        self._mapping = mapping

    def __iter__(self):
        for item in self._mapping.scan():
            key_values = self._mapping._key_values_from_item(item)
            key_values = simplify_tuple_keys(key_values)
            yield (key_values, item)


class DynamoDBMapping(Mapping):

    def __init__(
        self, table_name: str, boto3_session: Optional[boto3.Session]=None, **kwargs
    ) -> None:
        session = (
            boto3_session or
            kwargs.get("boto3_session") or
            boto3_session_from_config(kwargs) or
            boto3.Session()
        )
        dynamodb = session.resource("dynamodb")
        self._table = dynamodb.Table(table_name)
        self._key_names = get_key_names(self._table)

    def scan(self, **kwargs) -> Iterator:
        response = self._table.scan(**kwargs)
        for item in response["Items"]:
            yield item
        while "LastEvaluatedKey" in response:
            response = self._table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            for item in response["Items"]:
                yield item

    def get_item(self, keys, **kwargs) -> Dict:
        keys = create_tuple_keys(keys)
        if len(keys) != len(self._key_names):
            raise ValueError(f"You must provide a value for each of {self._key_names} keys.")
        key_param = {key: value for key, value in zip(self._key_names, keys)}
        response = self._table.get_item(Key=key_param, **kwargs)
        return response["Item"]

    def _key_values_from_item(self, item: Dict) -> Tuple:
        return tuple(item[key] for key in self._key_names)

    def __iter__(self) -> Iterator:
        for item in self.scan(ProjectionExpression=", ".join(self._key_names)):
            yield simplify_tuple_keys(self._key_values_from_item(item))

    def __len__(self) -> int:
        # WARNING: this is an approximate value, updated once in every approximately 6 hours.
        # If you need the exact current count of items, use len(list(dynamoDBMapping))
        return self._table.item_count

    def __getitem__(self, __key: Any) -> Any:
        return self.get_item(__key)

    def values(self) -> ValuesView:
        return DynamoDBValuesView(self)

    def items(self) -> ItemsView:
        return DynamoDBItemsView(self)


class DynamoDBConnection(ExperimentalBaseConnection[DynamoDBMapping]):
    """Connects a streamlit app to an Amazon DynamoDB table.


    """

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

    def _df_from_iterator(self, iterator: Iterator[Dict[str, Any]]) -> pd.DataFrame:
        df = pd.DataFrame(iterator)
        df.set_index(list(self.table._key_names), inplace=True)
        return df

    def scan(self, return_raw=False, **kwargs) -> Union[Iterator[Dict[str, Any]], pd.DataFrame]:
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
        iterator = self.table.scan(**kwargs)
        return iterator if return_raw else self._df_from_iterator(iterator)

    def get_item(self,
        keys: Union[str, List[str]],
        return_raw=False,
        **kwargs
    ) -> Union[Dict[str, Any], pd.Series]:
        item = self.table.get_item(keys, **kwargs)
        return item if return_raw else pd.Series(item, name="value")
