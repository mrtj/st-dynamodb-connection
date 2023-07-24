from typing import Any, Optional, Dict, Iterator, Union, List

from streamlit.connections import ExperimentalBaseConnection

import boto3
import pandas as pd

try:
    import mypy_boto3_dynamodb
    DynamoDBTable = mypy_boto3_dynamodb.service_resource.Table
except ImportError:
    DynamoDBTable = Any # type: ignore

class DynamoDBConnection(ExperimentalBaseConnection[DynamoDBTable]):

    def __init__(self,
        connection_name: str,
        boto3_session: Optional[boto3.Session]=None, **kwargs
    ) -> None:
        self.boto3_session = boto3_session
        super().__init__(connection_name, **kwargs)

    def _session_from_config(self, config: Dict[str, Any]) -> Optional[boto3.Session]:
        if "aws_access_key_id" in config and "aws_secret_access_key" in config:
            return boto3.Session(
                aws_access_key_id=config["aws_access_key_id"],
                aws_secret_access_key=config["aws_secret_access_key"],
                region_name=config.get("aws_region"),
                profile_name=config.get("aws_profile")
            )
        else:
            return None

    def _connect(self, **kwargs) -> DynamoDBTable:
        secrets = self._secrets.to_dict()
        table_name = kwargs.get("table_name") or secrets.get("table_name")
        if table_name is None:
            raise ValueError(
                "You must configure the DynamoDB table name either as a DynamoDBConnection secret "
                "called 'table_name' or pass it as a keyword parameter 'table_ name' when creating "
                "the connection."
            )

        session = (
            self.boto3_session or
            kwargs.get("boto3_session") or
            self._session_from_config(kwargs) or
            self._session_from_config(secrets) or
            boto3.Session()
        )
        dynamodb = session.resource("dynamodb")
        table = dynamodb.Table(table_name)
        return table

    def _get_keys(self) -> List[str]:
        keys: List[str] = []
        for schema_part in self._instance.key_schema:
            if schema_part["KeyType"] == "HASH":
                keys.insert(0, schema_part["AttributeName"])
            elif schema_part["KeyType"] == "RANGE":
                keys.append(schema_part["AttributeName"])
        return keys

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
        def _inner_scan() -> Iterator[Dict[str, Any]]:
            response = self._instance.scan(**kwargs)
            for item in response["Items"]:
                yield item
            while "LastEvaluatedKey" in response:
                response = self._instance.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                for item in response["Items"]:
                    yield item
        if return_raw:
            return _inner_scan()
        else:
            keys = self._get_keys()
            df = pd.DataFrame(_inner_scan())
            df.set_index(keys, inplace=True)
            return df
