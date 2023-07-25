from typing import Iterator, Tuple, List, Union, Any, Optional, Iterable, Dict
from collections.abc import ValuesView, ItemsView, Mapping

import boto3

from .utils import boto3_session_from_config

try:
    import mypy_boto3_dynamodb
    DynamoDBTable = mypy_boto3_dynamodb.service_resource.Table
except ImportError:
    DynamoDBTable = Any # type: ignore


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


class DynamoDBValuesView(ValuesView):

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
        if not "Item" in response:
            raise KeyError(simplify_tuple_keys(keys))
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

    def items(self) -> ItemsView:
        return DynamoDBItemsView(self)

    def values(self) -> ValuesView:
        return DynamoDBValuesView(self)