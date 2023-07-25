from typing import (
    Iterator, Tuple, Union, Any, Optional, Iterable, Dict, Set, List, Mapping, Sequence, cast
)
from collections.abc import ValuesView, ItemsView, MutableMapping
from decimal import Decimal

import boto3
from boto3.dynamodb.types import Binary

from .utils import boto3_session_from_config

try:
    import mypy_boto3_dynamodb
    DynamoDBTable = mypy_boto3_dynamodb.service_resource.Table
except ImportError:
    DynamoDBTable = Any # type: ignore

DynamoDBPrimitiveTypes = (str, bytes, bytearray, int, Decimal)
DynamoDBPrimitiveKey = Union[str, bytes, bytearray, int, Decimal]
DynamoDBSimpleKey = Tuple[DynamoDBPrimitiveKey]
DynamoDBCompositeKey = Tuple[DynamoDBPrimitiveKey, DynamoDBPrimitiveKey]
DynamoDBAnyKey = Union[DynamoDBSimpleKey, DynamoDBCompositeKey]
DynamoDBSimplifiedKey = Union[DynamoDBPrimitiveKey, DynamoDBCompositeKey]
DynamoDBKeyName = Union[Tuple[str], Tuple[str, str]]
DynamoDBValueTypes = (
    str, int, Decimal, Binary, bytes, bytearray, bool, None,
    Set[str], Set[int], Set[Decimal], Set[Binary], List, Dict
)
DynamoDBValue = Union[
    bytes, bytearray, str, int, Decimal,bool,
    Set[int], Set[Decimal], Set[str], Set[bytes], Set[bytearray],
    Sequence[Any], Mapping[str, Any], None,
]
DynamoDBItemType = Mapping[str, DynamoDBValue]

def get_key_names(table: DynamoDBTable) -> DynamoDBKeyName:
    """Gets the key names of the DynamoDB table.

    Returns:
        List[str]: A list with the key names. The list has either one (if only the hash key
            is defined on the table) or two (if both the hash and range key is defined)
            elements.
    """
    schema: Dict[str, str] = {s["KeyType"]: s["AttributeName"] for s in table.key_schema}
    return (schema["HASH"], schema["RANGE"]) if "RANGE" in schema else (schema["HASH"], )

def simplify_tuple_keys(tpl: DynamoDBAnyKey) -> Union[DynamoDBPrimitiveKey, DynamoDBCompositeKey]:
    if len(tpl) == 1:
        return tpl[0]
    else:
        return cast(DynamoDBCompositeKey, tpl)


def create_tuple_keys(key: DynamoDBSimplifiedKey) -> DynamoDBAnyKey:
    if not isinstance(key, DynamoDBPrimitiveTypes) and isinstance(key, Iterable):
        return cast(DynamoDBCompositeKey, tuple(key))
    else:
        return cast(DynamoDBSimpleKey, (key,))


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


class DynamoDBMapping(MutableMapping):

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

    def _create_key_param(self, keys: DynamoDBSimplifiedKey) -> Dict[str, DynamoDBPrimitiveKey]:
        tuple_keys = create_tuple_keys(keys)
        if len(tuple_keys) != len(self._key_names):
            raise ValueError(f"You must provide a value for each of {self._key_names} keys.")
        param = { name: value for name, value in zip(self._key_names, tuple_keys) }
        return param

    def scan(self, **kwargs) -> Iterator:
        response = self._table.scan(**kwargs)
        for item in response["Items"]:
            yield item
        while "LastEvaluatedKey" in response:
            response = self._table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            for item in response["Items"]:
                yield item

    def get_item(self, keys: DynamoDBSimplifiedKey, **kwargs) -> DynamoDBItemType:
        key_params = self._create_key_param(keys)
        response = self._table.get_item(Key=key_params, **kwargs)
        if not "Item" in response:
            log_keys: Union[List[str], str] = list(key_params.keys())
            if len(log_keys) == 1:
                log_keys = log_keys[0]
            raise KeyError(log_keys)
        return response["Item"]

    def set_item(self,
        keys: DynamoDBSimplifiedKey, item: DynamoDBItemType, **kwargs
    ) -> None:
        key_params = self._create_key_param(keys)
        _item = dict(**item, **key_params)
        self._table.put_item(Item=_item, **kwargs)

    def put_item(self,
        keys: DynamoDBSimplifiedKey, item: DynamoDBItemType, **kwargs
    ) -> None:
        self.set_item(keys, item, **kwargs)

    def del_item(self, keys: DynamoDBSimplifiedKey, **kwargs):
        key_params = self._create_key_param(keys)
        self._table.delete_item(Key=key_params, **kwargs)

    def _key_values_from_item(self, item: Dict) -> DynamoDBAnyKey:
        return cast(DynamoDBAnyKey, tuple(item[key] for key in self._key_names))

    def __iter__(self) -> Iterator:
        for item in self.scan(ProjectionExpression=", ".join(self._key_names)):
            yield simplify_tuple_keys(self._key_values_from_item(item))

    def __len__(self) -> int:
        # WARNING: this is an approximate value, updated once in every approximately 6 hours.
        # If you need the exact current count of items, use len(list(dynamoDBMapping))
        return self._table.item_count

    def __getitem__(self, __key: Any) -> Any:
        return self.get_item(__key)

    def __setitem__(self, __key: Any, __value: Any) -> None:
        self.set_item(__key, __value)

    def __delitem__(self, __key: Any) -> None:
        self.del_item(__key)

    def items(self) -> ItemsView:
        return DynamoDBItemsView(self)

    def values(self) -> ValuesView:
        return DynamoDBValuesView(self)
