#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use BlockStorage WebApps
"""
import os
import sys
import array
import logging
import hashlib
import json
from io import BytesIO
# non std modules
import yaml
import boto3
from botocore.exceptions import ClientError
# own modules
from .Checksums import Checksums


class StorageClient():
    """stores chunks of data into BlockStorage"""

    def __init__(self):
        self._homepath = None
        self._config = None
        self._checksums = None
        self._logger = None
        # according to platform search for config file in home directory
        if os.name == "nt":
            self._homepath = os.path.join(os.path.expanduser("~"), "AppData", "Local", "webstorage")
        else:
            self._homepath = os.path.join(os.path.expanduser("~"), ".webstorage")
        logging.debug("using config directory %s", self._homepath)
        if not os.path.isdir(self._homepath):
            print(f"first create directory {self._homepath} and place webstorage.yml file in there")
            sys.exit(1)
        configfile = os.path.join(self._homepath, "webstorage.yml")
        if os.path.isfile(configfile):
            with open(configfile, "rt") as infile:
                self._config = yaml.load(infile.read())
                # use proxy, if defined in config
                if "HTTP_PROXY" in self._config:
                    os.environ["HTTP_PROXY"] = self._config["HTTP_PROXY"]
                if "HTTPS_PROXY" in self._config:
                    os.environ["HTTPS_PROXY"] = self._config["HTTPS_PROXY"]
                self._client = boto3.client(
                    "s3",
                    aws_access_key_id=self._config["S3_ACCESS_KEY"],
                    aws_secret_access_key=self._config["S3_SECRET_KEY"],
                    endpoint_url=self._config["S3_ENDPOINT_URL"],
                    use_ssl=self._config["S3_USE_SSL"]
                    )
        else:
            print(f"configuration file {configfile} is missing")
            sys.exit(2)

    def __contains__(self, checksum):
        return checksum in self._checksums

    def _blockdigest(self, data):
        """
        single point of digesting return hexdigest of data

        :param data <bytes>: some data
        """
        digest = self.hashfunc()
        digest.update(data)
        return digest.hexdigest()

    @property
    def blocksize(self):
        """
        return blocksize
        """
        return self._blocksize

    @property
    def checksums(self):
        """
        return list of known checksums
        """
        return self._checksums.checksums()

    def _get_checksums(self):
        """
        get list of stored checksums from backend
        """
        # Create a reusable Paginator
        if len(self._checksums) == 0:
            self._logger.info("no locally stored checksums found, fetching from bucket")
            paginator = self._client.get_paginator('list_objects')
            # Create a PageIterator from the Paginator
            page_iterator = paginator.paginate(Bucket=self._bucket_name)
            checksums = set()
            for page in page_iterator:
                for entry in page["Contents"]:
                    if entry["Key"] not in self._checksums:
                        checksums.add(entry["Key"])
            self._checksums.update(checksums)
        self._logger.info(f"found {len(self._checksums)} existing checksums")
