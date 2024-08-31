import os

import boto3
import botocore.exceptions


def get_parameter(key_name):
    try:
        ssm = boto3.client("ssm")
        value = ssm.get_parameter(Name=key_name)["Parameter"]["Value"]
    except (botocore.exceptions.NoRegionError, ssm.exceptions.ParameterNotFound):
        value = os.environ[key_name]
    return value


def put_parameter(key_name, value):
    try:
        ssm = boto3.client("ssm")
        ssm.put_parameter(Name=key_name, Value=value, Type="String", Overwrite=True)
    except botocore.exceptions.NoRegionError:
        pass
