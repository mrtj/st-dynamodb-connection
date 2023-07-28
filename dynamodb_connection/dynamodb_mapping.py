"""DynamoDBMapping allows you to use an Amazon DynamoDB table as simply as if it were a Python
dictionary."""

from typing import (
    Iterator, KeysView, Tuple, Union, Any, Optional, Iterable, Dict, Set, List, Mapping, Sequence, cast
)
from collections.abc import ValuesView, ItemsView, KeysView, MutableMapping
from decimal import Decimal
import logging

import boto3
from boto3.dynamodb.types import Binary

from .utils import boto3_session_from_config

try:
    import mypy_boto3_dynamodb
    DynamoDBTable = mypy_boto3_dynamodb.service_resource.Table
except ImportError:
    DynamoDBTable = Any # type: ignore

DynamoDBKeyPrimitiveTypes = (str, bytes, bytearray, int, Decimal)
"""DynamoDB primary key primitive choices."""

DynamoDBKeyPrimitive = Union[str, bytes, bytearray, int, Decimal]
"""DynamoDB primary key primitive."""

DynamoDBKeySimple = Tuple[DynamoDBKeyPrimitive]
"""DynamoDB simple primary key type (a partition key only)."""

DynamoDBKeyComposite = Tuple[DynamoDBKeyPrimitive, DynamoDBKeyPrimitive]
"""DynamoDB composite primary key type (a partition key and a sort key)."""

DynamoDBKeyAny = Union[DynamoDBKeySimple, DynamoDBKeyComposite]
"""Any DynamoDB primary key type."""

DynamoDBKeySimplified = Union[DynamoDBKeyPrimitive, DynamoDBKeyComposite]
"""A simplified DynamoDB key type: a primitive in case of simple primary key,
and a tuple in the case of composite key."""

DynamoDBKeyName = Union[Tuple[str], Tuple[str, str]]
"""DynamoDB primary key name type"""

DynamoDBValueTypes = (
    str, int, Decimal, Binary, bytes, bytearray, bool, None,
    Set[str], Set[int], Set[Decimal], Set[Binary], List, Dict
)
"""DynamoDB value type choices."""

DynamoDBValue = Union[
    bytes, bytearray, str, int, Decimal,bool,
    Set[int], Set[Decimal], Set[str], Set[bytes], Set[bytearray],
    Sequence[Any], Mapping[str, Any], None,
]
"""DynamoDB value type."""

DynamoDBItemType = Mapping[str, DynamoDBValue]
"""DynamoDB item type."""

logger = logging.getLogger(__name__)

def get_key_names(table: DynamoDBTable) -> DynamoDBKeyName:
    """Gets the key names of the DynamoDB table.

    Args:
        table (DynamoDBTable): The DynamoDB table.

    Returns:
        DynamoDBKeyName: A tuple with either one (if only the partition key is defined on the table)
            or two (if both the partition and range key is defined) elements.
    """
    schema: Dict[str, str] = {s["KeyType"]: s["AttributeName"] for s in table.key_schema}
    return (schema["HASH"], schema["RANGE"]) if "RANGE" in schema else (schema["HASH"], )


def simplify_tuple_keys(key: DynamoDBKeyAny) -> DynamoDBKeySimplified:
    """Simplifies an arbitrary DynamoDB key.

    If the key is simple, it is returned as a primitive. If it is a composite key, it is returned
    as a two-element tuple.

    Args:
        key (DynamoDBKeyAny): Any DynamoDB key type.

    Returns:
        DynamoDBKeySimplified: The simplified key.
    """
    if len(key) == 1:
        return key[0]
    else:
        return cast(DynamoDBKeyComposite, key)


def create_tuple_keys(key: DynamoDBKeySimplified) -> DynamoDBKeyAny:
    """Creates a well-defined DynamoDB key from a simplified key.

    If the simplified key is of a primitive type, it is returned as a one-element tuple. If it
    is a composite key, it is returned as a two-element tuple. This method is effectively the
    inverse of simplify_tuple_keys.

    Args:
        key (DynamoDBKeySimplified): The simplified key, either a primitive key value, or a tuple.

    Returns:
        DynamoDBKeyAny: The well-defined DynamoDB key.
    """
    if not isinstance(key, DynamoDBKeyPrimitiveTypes) and isinstance(key, Iterable):
        return cast(DynamoDBKeyComposite, tuple(key))
    else:
        return cast(DynamoDBKeySimple, (key,))

def _log_keys_from_params(key_params: Dict[str, DynamoDBKeyPrimitive]) -> str:
    log_keys = list(key_params.values())
    res = log_keys[0] if len(log_keys) == 1 else log_keys
    return str(res)


class DynamoDBValuesView(ValuesView):
    """Efficient implementation of python dict ValuesView on DynamoDBMapping types.

    The original implementation of ValuesView would first call a scan operation on the table,
    discard everything except the key values, and then call a get_item operation on each key.
    This implementation calls only scan once.
    """

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
    """Efficient implementation of python dict ItemsView on DynamoDBMapping types.

    The original implementation of ValuesView would first call a scan operation on the table,
    discard everything except the key values, and then call a get_item operation on each key.
    This implementation calls only scan once.
    """

    def __init__(self, mapping: "DynamoDBMapping") -> None:
        self._mapping = mapping

    def __iter__(self):
        for item in self._mapping.scan():
            key_values = self._mapping._key_values_from_item(item)
            key_values = simplify_tuple_keys(key_values)
            yield (key_values, item)


class DynamoDBKeysView(KeysView):
    """Efficient implementation of python dict KeysView on DynamoDBMapping types."""

    def __init__(self, mapping: "DynamoDBMapping") -> None:
        self._mapping = mapping

    def __contains__(self, key: object) -> bool:
        try:
            self._mapping[key]
        except KeyError:
            return False
        else:
            return True


class DynamoDBItemAccessor(dict):
    """This subclass of dictionary ensures the effective update of the DynamoDB table when a field
    of the item returned by `get_item` is modified.

    This is an internal helper class and most likely, users of `DynamoDBMapping` will not need to
    use it.

    Args:
        parent (DynamoDBMapping): The parent mapping that created this accessor.
        item_keys (DynamoDBKeySimplified): The keys of the item this accessor is modifying.
        initial_data (Dict): The initial item data.
    """

    def __init__(self,
        parent: "DynamoDBMapping",
        item_keys: DynamoDBKeySimplified,
        initial_data: DynamoDBItemType,
    ) -> None:
        self._parent = parent
        self._item_keys = item_keys
        super().__init__(initial_data)

    def __setitem__(self, __key: Any, __value: Any) -> None:
        self._parent.modify_item(self._item_keys, {__key: __value})
        return super().__setitem__(__key, __value)


class DynamoDBMapping(MutableMapping):
    """DynamoDBMapping is an alternative API for Amazon DynamoDB that implements the Python
    MutableMapping abstract base class, effectively allowing you to use a DynamoDB table as if it
    were a Python dictionary.

    You have the following options to configure the underlying boto3 session:

    - Automatic configuration: pass nothing to DynamoDBMapping initializer. This will prompt
    DynamoDBMapping to load the default `Session` object, which in turn will use the standard boto3
    credentials chain to find AWS credentials (e.g., the ~/.aws/credentials file, environment
    variables, etc.).
    - Pass a preconfigured boto3 `Session` object
    - Pass `aws_access_key_id` and `aws_secret_access_key` as keyword arguments. Additionally,
    the optional `aws_region` and `aws_profile` arguments are also considered.

    Example:
    ```python
    from dynamodb_mapping import DynamoDBMapping
    mapping = DynamoDBMapping(table_name="my_table")

    # Iterate over all items:
    for key, value in mapping.items():
        print(key, value)

    # Get a single item:
    print(mapping["my_key"])

    # Create or modify an item:
    mapping["my_key"] = {"description": "foo", "price": 123}

    # Delete an item:
    del mapping["my_key"]
    ```

    All methods that iterate over the elements of the table do so in a lazy manner, in that the
    successive pages of the scan operation are queried only on demand. Examples of such operations
    include scan, iteration over keys, iteration over values, and iteration over items (key-value
    tuples). You should pay particular attention to certain patterns that fetch all items in the
    table, for example, calling list(mapping.values()). This call will execute an exhaustive scan on
    your table, which can be costly, and attempt to load all items into memory, which can be
    resource-demanding if your table is particularly large.

    The `__len__` implementation of this class returns a best-effort estimate of the number of
    items in the table using the TableDescription DynamoDB API. The number of items are updated
    at DynamoDB service side approximately once in every 6 hours. If you need the exact number of
    items currently in the table, you can use `len(list(mapping.keys()))`. Note however that this
    will cause to run an exhaustive scan operation on your table.

    Args:
        table_name (str): The name of the DynamoDB table.
        boto3_session (Optional[boto3.Session]): An optional preconfigured boto3 Session object.
        **kwargs: Additional keyword parameters for manual configuration of the boto3 client:
            `aws_access_key_id`, `aws_secret_access_key`, `aws_region`, `aws_profile`.
    """

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
        self.table = dynamodb.Table(table_name)
        self.key_names = get_key_names(self.table)

    def _create_key_param(self, keys: DynamoDBKeySimplified) -> Dict[str, DynamoDBKeyPrimitive]:
        tuple_keys = create_tuple_keys(keys)
        if len(tuple_keys) != len(self.key_names):
            raise ValueError(f"You must provide a value for each of {self.key_names} keys.")
        param = { name: value for name, value in zip(self.key_names, tuple_keys) }
        return param

    def scan(self, **kwargs) -> Iterator[DynamoDBItemType]:
        """Performs a scan operation on the DynamoDB table. The scan is executed in a lazy manner,
        in that the successive pages are queried only on demand.

        Args:
            **kwargs: keyword arguments to be passed to the underlying DynamoDB scan operation.

        Returns:
            Iterator[DynamoDBItemType]: An iterator over all items in the table.
        """
        logger.debug("Performing a scan operation on %s table", self.table.name)
        response = self.table.scan(**kwargs)
        for item in response["Items"]:
            yield item
        while "LastEvaluatedKey" in response:
            response = self.table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            for item in response["Items"]:
                yield item

    def get_item(self, keys: DynamoDBKeySimplified, **kwargs) -> DynamoDBItemType:
        """Retrieves a single item from the table.

        The value(s) of the item's key(s) should be specified.

        Args:
            keys (DynamoDBKeySimplified): The key value. This can either be a simple Python type,
                if only the partition key is specified in the table's key schema, or a tuple of the
                partition key and the range key values, if both are specified in the key schema.
            **kwargs: keyword arguments to be passed to the underlying DynamoDB get_item operation.

        Raises:
            ValueError: If the required key values are not specified.
            KeyError: If no item can be found under this key in the table.

        Returns:
            DynamoDBItemType: A single item from the table.
        """
        key_params = self._create_key_param(keys)
        logger.debug("Performing a get_item operation on %s table", self.table.name)
        response = self.table.get_item(Key=key_params, **kwargs)
        if not "Item" in response:
            raise KeyError(_log_keys_from_params(key_params))
        data = response["Item"]
        return DynamoDBItemAccessor(parent=self, item_keys=keys, initial_data=data)

    def set_item(self, keys: DynamoDBKeySimplified, item: DynamoDBItemType, **kwargs) -> None:
        """Creates or overwrites a single item in the table.

        Args:
            keys (DynamoDBKeySimplified): The key value. This can either be a simple Python type,
                if only the partition key is specified in the table's key schema, or a tuple of the
                partition key and the range key values, if both are specified in the key schema.
            item (DynamoDBItemType): The new item.
            **kwargs: keyword arguments to be passed to the underlying DynamoDB set_item operation.

        """
        key_params = self._create_key_param(keys)
        _item = dict(**item, **key_params)
        logger.debug("Performing a put_item operation on %s table", self.table.name)
        self.table.put_item(Item=_item, **kwargs)

    def put_item(self, keys: DynamoDBKeySimplified, item: DynamoDBItemType, **kwargs) -> None:
        """An alias for the `set_item` method."""
        self.set_item(keys, item, **kwargs)

    def del_item(self, keys: DynamoDBKeySimplified, check_existing=True, **kwargs) -> None:
        """Deletes a single item from the table.

        Args:
            keys (DynamoDBKeySimplified): The key value. This can either be a simple Python type,
                if only the partition key is specified in the table's key schema, or a tuple of the
                partition key and the range key values, if both are specified in the key schema.
            check_existing (bool): Raise ValueError if the specified key does not exists in the
                table. Defaults to True to be consistent with python dict implementation, however
                this causes an additional get_item operation to be executed.
            **kwargs: keyword arguments to be passed to the underlying DynamoDB delete_item
                operation.
        """
        key_params = self._create_key_param(keys)
        if check_existing and not keys in self.keys():
            raise KeyError(_log_keys_from_params(key_params))
        logger.debug("Performing a delete_item operation on %s table", self.table.name)
        self.table.delete_item(Key=key_params, **kwargs)

    def modify_item(self,
        keys: DynamoDBKeySimplified,
        modifications: DynamoDBItemType,
        **kwargs
    ) -> None:
        """Modifies the properties of an existing item.

        Args:
            keys (DynamoDBKeySimplified): The key value of the item. This can either be a simple
                Python type, if only the partition key is specified in the table's key schema, or a
                tuple of the partition key and the range key values, if both are specified in the
                key schema.
            modifications (DynamoDBItemType): A mapping containing the desired modifications to
                the fields of the item. This mapping follows the same format as the entire item,
                but it isn't required to contain all fields: fields that are omitted will be
                unaffected. To delete a field, set the field value to None.
            **kwargs: keyword arguments to be passed to the underlying DynamoDB update_item
                operation.
        """
        key_params = self._create_key_param(keys)
        set_expression_parts = []
        remove_expression_parts = []
        attribute_names = {}
        attribute_values = {}
        for idx, (attrib_key, attrib_value) in enumerate(modifications.items()):
            attrib_key_ph = f"#key{idx}"
            attrib_value_ph = f":value{idx}"
            attribute_names[attrib_key_ph] = attrib_key
            if attrib_value is None:
                remove_expression_parts.append(attrib_key_ph)
            else:
                set_expression_parts.append(f"{attrib_key_ph} = {attrib_value_ph}")
                attribute_values[attrib_value_ph] = attrib_value
        update_expression_parts = []
        if set_expression_parts:
            update_expression_parts.append("set " + ", ".join(set_expression_parts))
        if remove_expression_parts:
            update_expression_parts.append("remove " + ", ".join(remove_expression_parts))
        if not update_expression_parts:
            logger.warning(
                "No update expression was created by modify_item: modifications mapping is empty?"
            )
            return
        update_expression = " ".join(update_expression_parts)
        logger.debug(
            "Performing an update_item operation on %s table with update expression %s",
            self.table.name,
            update_expression,
        )
        update_item_kwargs = {
            **kwargs,
            "Key": key_params,
            "UpdateExpression": update_expression,
        }
        if attribute_values:
            update_item_kwargs["ExpressionAttributeValues"] = attribute_values
        if attribute_names:
            update_item_kwargs["ExpressionAttributeNames"] = attribute_names
        self.table.update_item(**update_item_kwargs)

    def _key_values_from_item(self, item: DynamoDBItemType) -> DynamoDBKeyAny:
        return cast(DynamoDBKeyAny, tuple(item[key] for key in self.key_names))

    def __iter__(self) -> Iterator:
        """Returns an iterator over the table.

        This method performs a lazy DynamoDB `scan` operation.
        """
        for item in self.scan(ProjectionExpression=", ".join(self.key_names)):
            yield simplify_tuple_keys(self._key_values_from_item(item))

    def __len__(self) -> int:
        """Returns a best effort estimation of the number of items in the table.

        If you need the precise number of items in the table, you can use
        `len(list(mapping.keys()))`. However this later can be a costly operation.
        """
        return self.table.item_count

    def __getitem__(self, __key: Any) -> Any:
        """Retrieves a single item from the table.

        Delegates the call to `get_item` method without additional keyword arguments.
        """
        return self.get_item(__key)

    def __setitem__(self, __key: Any, __value: Any) -> None:
        """Creates or overwrites a single item in the table.

        Delegates the call to `set_item` method without additional keyword arguments.
        """
        self.set_item(__key, __value)

    def __delitem__(self, __key: Any) -> None:
        """Deletes a single item from the table.

        Delegates the call to `del_item` method without additional keyword arguments.
        """
        self.del_item(__key)

    def items(self) -> ItemsView:
        """Returns an efficient implementation of the ItemsView on this table.

        The returned view can be used to iterate over (key, value) tuples in the table.

        Returns:
            ItemsView: The items view.
        """
        return DynamoDBItemsView(self)

    def values(self) -> ValuesView:
        """Returns an efficient implementation of the ValuesView on this table.

        The returned view can be used to iterate over the values in the table.

        Returns:
            ValuesView: The values view.
        """
        return DynamoDBValuesView(self)

    def keys(self) -> KeysView:
        """Returns an efficient implementation of the KeysView on this table.

        The returned view can be used to iterate over the keys in the table.

        Returns:
            KeysView: The keys view.
        """
        return DynamoDBKeysView(self)
