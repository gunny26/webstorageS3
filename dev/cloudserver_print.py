#!/usr/bin/python3
import logging
import uuid
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)

access_key = "214398ae-11fb-4fe3-b516-3b566973ddfe"
secret_key = "ec0386992ce254382e57ac2a4682b8ae65d794a09322a1054e1a4aea42e0448e"
endpoint_url = "http://docker.messner.click:8000"

client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        use_ssl=False
        )
res = client.list_buckets()
for bucket_name in res["Buckets"]:
    print(bucket_name["Name"])
    l_objects = client.list_objects_v2(Bucket=bucket_name["Name"])
    if "Contents" in l_objects:
        print(l_objects["Contents"])
        for key in l_objects["Contents"]:
            print(key)

