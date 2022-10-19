#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use BlockStorage WebApps
"""
import os
import sys
import logging
import hashlib
from io import BytesIO
# non std modules
import yaml
import boto3
import botocore

logger = logging.getLogger("StorageClient")


class StorageClient():
    """stores chunks of data into BlockStorage"""

    def __init__(self, homepath=None, s3_backend="DEFAULT"):
        logger.debug(f"s3_backend = {s3_backend}")
        logger.debug(f"homepath   = {homepath}")

        self._config = None  # holding yaml cnfig
        self._checksums = None  # checksum cache
        self._logger = None  # logger
        self._bucket_name = None   # bucket_name in S3
        self._hashfunc = hashlib.sha1  # TODO: hardcoded or in config?
        self._blocksize = 1024 * 1024  # TODO: hardcoded or in config?

        # according to platform search for config file in home directory
        self._homepath = None
        if not homepath:
            if os.name == "nt":
                self._homepath = os.path.join(os.path.expanduser("~"), "AppData", "Local", "webstorage")
            else:
                self._homepath = os.path.join(os.path.expanduser("~"), ".webstorage")
        else:
            self._homepath = homepath
        logger.debug(f"using config directory {self._homepath}")
        if not os.path.isdir(self._homepath):
            logger.error(f"create directory {self._homepath} and place webstorage.yml file in there")
            sys.exit(1)

        # checking configfile
        configfile = os.path.join(self._homepath, "webstorage.yml")
        if os.path.isfile(configfile):
            with open(configfile, "rt") as infile:
                try:
                    data = yaml.safe_load(infile.read())
                    logger.debug(yaml.dump(data, indent=2))
                    self._config = data["S3Backends"][s3_backend]  # otherwise KeyError - invalid Config
                except KeyError as exc:
                    logger.exception(exc)
                    logger.error(f"invalid config file format, at least key S3Backends and Subkey DEFAULT must exist")
                    sys.exit(2)
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
            logger.error(f"configuration file {configfile} is missing")
            sys.exit(2)

    @property
    def hashfunc(self):
        """ returning used hashfunc """
        return self._hashfunc

    @property
    def homepath(self):
        """where to find config and cache"""
        return self._homepath

    @property
    def blocksize(self):
        """ return blocksize """
        return self._blocksize

    @property
    def checksums(self):
        """ return list of known checksums """
        return list(self._checksums.checksums())

    def __contains__(self, checksum):
        return checksum in self._checksums

    def _blockdigest(self, data):
        """
        single point of digesting some data returning hexdigest of data

        :param data <bytes>: some data
        """
        digest = self._hashfunc()
        digest.update(data)
        return digest.hexdigest()

    def _download_fileobj(self, key):
        """
        download some data from S3

        :param bucket_name <str>: some existing bucket in S3
        :param key <str>: key of object in bucket
        :return <bytes> binary data of object:
        """
        b_buffer = BytesIO()
        self._client.download_fileobj(self._bucket_name, key, b_buffer)  # TODO: exceptions
        b_buffer.seek(0)  # do not forget this tiny little line !!
        return b_buffer.read()

    def _list_objects(self):
        """
        generator to return objects in bucket

        :param bucket <str>: name of bucket
        :return <generator> of entry["Key"] of objects
        """
        paginator = self._client.get_paginator('list_objects')
        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(Bucket=self._bucket_name)
        for page in page_iterator:
            if page.get("Contents"):
                for entry in page["Contents"]:
                    yield entry["Key"]

    def _get_checksums(self):
        """
        get list of stored checksums from backend
        """
        # Create a reusable Paginator
        # TODO: put this in special function if necessary
        # if len(self._checksums) == 0:  # must be len() to check of no entry
        #     self._logger.info("no locally stored checksums found, fetching from bucket")
        #     self._checksums.update(set(self._list_objects()))
        self._logger.info(f"found {len(self._checksums)} existing checksums")

    def _exist(self, key, force=False):
        """
        checking if key exists in bucket if force=True
        otherwise only checking local stored checksums
        """
        if not force:
            return key in self._checksums
        try:
            self._client.head_object(Bucket=self._bucket_name, Key=key)
            if key not in self._checksums:  # add to local cache
                self._checksums.add(key)
            return True
        except botocore.exceptions.ClientError as exc:
            if exc.response['Error']['Code'] == "404":
                return False
            else:
                # Something else has gone wrong.
                raise exc
