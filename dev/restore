#!/usr/bin/python3
import logging
import uuid
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)

access_key = "214398ae-11fb-4fe3-b516-3b566973ddfe"
secret_key = "ec0386992ce254382e57ac2a4682b8ae65d794a09322a1054e1a4aea42e0448e"
endpoint_url = "http://docker.messner.click:8000"
buckets = [
    "webstorage-f6635276-9e5e-4d7a-a034-1e34293c7e28",
    "filestorage-a0287c41-1fcc-49d3-a0c8-71534064222a",
    "blockstorage-442e25c7-c1c9-49e1-9348-2574d8d06e61"
    ]

client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url
        )
res = client.list_buckets()
for bucket_name in res["Buckets"]:
    print(bucket_name["Name"])
print(f"listing available buckets: {client.list_buckets()}")
for bucket in buckets:
    client.create_bucket(Bucket=bucket)
    res = client.upload_file("./cloudserver_init.py", bucket, "init")
    client.download_file(bucket, "init", "restore")

