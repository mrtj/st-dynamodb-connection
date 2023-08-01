from typing import Dict, Any, Optional

import boto3

def get_case_insensitive(key: str, config: Dict[str, Any]) -> Optional[Any]:
    return config.get(key) or config.get(key.upper())

def boto3_session_from_config(config: Dict[str, Any]) -> Optional[boto3.Session]:
    aws_access_key_id = get_case_insensitive("aws_access_key_id", config)
    aws_secret_access_key = get_case_insensitive("aws_secret_access_key", config)
    aws_region = get_case_insensitive("aws_region", config)
    aws_profile = get_case_insensitive("aws_profile", config)
    if aws_access_key_id is not None and aws_secret_access_key is not None:
        return boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
            profile_name=aws_profile,
        )
    else:
        return None
