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


class BlockStorageError(Exception):
    pass


class BlockStorageClient():
    """stores chunks of data into BlockStorage"""

    def __init__(self, cache=True):
        """__init__"""
        self._logger = logging.getLogger(self.__class__.__name__)
        self._cache = cache
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
            with open(configfile) as infile:
                self._config = yaml.load(infile)
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
        self._bucket_name = self._config["BLOCKSTORAGE_BUCKET_NAME"]
        self.hashfunc = hashlib.sha1 # TODO: hardcoded or in config?
        self._blocksize = 1024 * 1024 # TODO: hardcoded or in config?
        self._logger.debug("bucket list: %s", self._client.list_buckets())
        self._checksums = []
        self._get_checksums()
        self._logger.debug("found %d stored checksums", len(self._checksums))

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
        if self._cache and not self._checksums:
            self._logger.info("getting existing checksums")
            self._get_checksums()
        return self._checksums

    def _get_checksums(self):
        """
        get checksums from backend
        """
        objects = self._client.list_objects(Bucket=self._bucket_name) # TODO: exceptions
        if "Contents" in objects:
            for entry in objects['Contents']:
                if entry["Key"] not in self._checksums:
                    self._checksums.append(entry["Key"])

    def put(self, data, use_cache=False):
        """
        put some arbitrary data into storage

        :param data <bytes>: arbitrary data up to blocksize long
        :param use_cache <bool>: if checksum already in list of checksums, do not store, otherwise overwrite
        """
        if len(data) > self.blocksize: # assure maximum length
            raise BlockStorageError("length of providede data (%s) is above maximum blocksize of %s" % (len(data), self.blocksize))
        checksum = self._blockdigest(data)
        if use_cache and checksum in self._checksums:
            self._logger.debug("202 - skip this block, checksum is in list of cached checksums")
            return checksum, 202
        self._client.upload_fileobj(BytesIO(data), self._bucket_name, checksum) # TODO: exceptions
        self._checksums.append(checksum) # add to local cache
        return checksum, 200 # fake

    def get(self, checksum, verify=False):
        """
        get data defined by hexdigest from storage
        if verify - recheck checksum locally

        :param checksum <str>: hexdigest of data
        :param verify <bool>: to verify checksum locally, or not
        """
        b_buffer = BytesIO()
        self._client.download_fileobj(self._bucket_name, checksum, b_buffer) # TODO: exceptions
        b_buffer.seek(0) # do not forget this tiny little line !!
        data = b_buffer.read()
        if verify:
            if checksum != self._blockdigest(data):
                raise BlockStorageError("Checksum mismatch %s requested, %s get" % (checksum, self._blockdigest(data)))
        return data

    def get_verify(self, checksum):
        """
        get data with checksum and verify checksum before returning

        :param checksum <str>: hexdigest of checksum
        """
        return self.get(checksum, verify=True)

    def exists(self, checksum):
        """
        exists method if caching is on
        if the searched checksum is not available, the filestorage backend is queried

        :param checksum <str>: hexdigest of checksum
        """
        return checksum in self._checksums # check local cache first
