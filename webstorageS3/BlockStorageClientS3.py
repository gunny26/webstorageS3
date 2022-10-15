#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use BlockStorage WebApps
"""
import os
import logging
from io import BytesIO
# own modules
from .Checksums import Checksums
from .StorageClientS3 import StorageClient


class BlockStorageError(Exception):
    pass


class BlockStorageClient(StorageClient):
    """stores chunks of data into BlockStorage"""

    CACHE_FILENAME = "_blockstorage_cache.db"  # filname to store checksums

    def __init__(self, cache=True, homepath=None):
        """__init__"""
        super(BlockStorageClient, self).__init__(cache, homepath)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._bucket_name = self._config["BLOCKSTORAGE_BUCKET_NAME"]
        self._logger.debug(f"bucket list: {self._client.list_buckets()}")
        if cache:
            self._checksums = Checksums(os.path.join(self._homepath, self.CACHE_FILENAME))
            self._get_checksums()
            self._logger.debug("found {len(self._checksums)} stored checksums")
        else:
            self._checksums = set()
            self._logger.info("cache disabled")

    @property
    def blocksize(self):
        """
        return blocksize
        """
        return self._blocksize

    def put(self, data, use_cache=False):
        """
        put some arbitrary data into storage

        :param data <bytes>: arbitrary data up to blocksize long
        :param use_cache <bool>: if checksum already in list of checksums, do not store, otherwise overwrite
        """
        if len(data) > self.blocksize:  # assure maximum length
            raise BlockStorageError("length of providede data (%s) is above maximum blocksize of %s" % (len(data), self.blocksize))
        checksum = self._blockdigest(data)
        if use_cache and checksum in self._checksums:
            self._logger.debug("202 - skip this block, checksum is in list of cached checksums")
            return checksum, 202
        self._client.upload_fileobj(BytesIO(data), self._bucket_name, checksum)  # TODO: exceptions
        self._checksums.add(checksum)  # add to local cache
        return checksum, 200  # fake

    def get(self, checksum, verify=False):
        """
        get data defined by hexdigest from storage
        if verify - recheck checksum locally

        :param checksum <str>: hexdigest of data
        :param verify <bool>: to verify checksum locally, or not
        """
        b_buffer = BytesIO()
        self._client.download_fileobj(self._bucket_name, checksum, b_buffer)  # TODO: exceptions
        b_buffer.seek(0)  # do not forget this tiny little line !!
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

    def exist(self, checksum, force=False):
        """
        return True if checksum is in local cache
        TODO: also check S3 Backend ?

        :param checksum <str>: hexdigest of checksum
        :param force <bool>:if True check S3 backend
        :return <bool>: True if checksum also known
        """
        return self._exist(checksum, force)

    def purge_cache(self):
        """
        delete locally cached checksums
        """
        cache_filename = os.path.join(self._homepath, self.CACHE_FILENAME)
        self._logger.info(f"deleting local cached checksum database in file {cache_filename}")
        del(self._checksums)  # to close database and release file
        os.unlink(cache_filename)
        self._checksums = Checksums(cache_filename)
