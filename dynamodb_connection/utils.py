from typing import Dict, Any, Optional

import boto3

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