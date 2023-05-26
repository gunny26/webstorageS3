#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use BlockStorage WebApps
"""
import logging
import os
from io import BytesIO

# own modules
from .Checksums import Checksums
from .StorageClientS3 import StorageClient


class BlockStorageError(Exception):
    pass


class BlockStorageClient(StorageClient):
    """stores chunks of data into BlockStorage"""

    def __init__(self, homepath: str, cache: bool = True, s3_backend: str = "DEFAULT"):
        """__init__"""
        super(BlockStorageClient, self).__init__(
            homepath=homepath, s3_backend=s3_backend
        )
        self._cache = cache  # bool to indicate if persistent cache is used
        self._logger = logging.getLogger(self.__class__.__name__)
        self._bucket_name = self._config["BLOCKSTORAGE_BUCKET_NAME"]
        self._logger.info(f"{s3_backend} bucket to use: {self._bucket_name}")

        self._check_bucket()
        self._init_cache(cache, "blockstorage")

        # caching is done in this class, base class does not provide any
        # cache
        #subdir = os.path.join(self._homepath, ".cache")
        #self._cache_filename = os.path.join(subdir, f"{s3_backend}_blockstorage.db")
        #if cache:
        #    if not os.path.isdir(subdir):
        #        os.mkdir(subdir)
        #    self._cache = Checksums(self._cache_filename)
        #else:
        #    self._cache = set()
        #    self._logger.info("persistent cache disabled")

    @property
    def cache(self):
        return self._cache

    def put(self, data: str, use_cache: bool = False):
        """
        put some arbitrary data into storage

        :param data <bytes>: arbitrary data up to blocksize long
        :param use_cache <bool>: if checksum already in list of checksums, do not store, otherwise overwrite
        """
        if len(data) > self.blocksize:  # assure maximum length
            raise BlockStorageError(
                "length of providede data (%s) is above maximum blocksize of %s"
                % (len(data), self.blocksize)
            )
        checksum = self._blockdigest(data)
        if use_cache and (checksum in self._cache):
            self._logger.debug(
                "202 - skip this block, checksum is in list of cached checksums"
            )
            return checksum, 202
        self._client.upload_fileobj(
            BytesIO(data), self._bucket_name, checksum
        )  # TODO: exceptions

        self._cache.add(checksum)  # add to local cache
        return checksum, 200  # fake

    def get(self, checksum: str, verify: bool = False):
        """
        get data defined by hexdigest from storage
        if verify - recheck checksum locally

        :param checksum <str>: hexdigest of data
        :param verify <bool>: to verify checksum locally, or not
        """
        b_buffer = BytesIO()
        self._client.download_fileobj(
            self._bucket_name, checksum, b_buffer
        )  # TODO: exceptions
        b_buffer.seek(0)  # do not forget this tiny little line !!
        data = b_buffer.read()
        if verify:
            if checksum != self._blockdigest(data):
                raise BlockStorageError(
                    "Checksum mismatch %s requested, %s get"
                    % (checksum, self._blockdigest(data))
                )

        self._cache.add(checksum)  # add to local cache
        return data

    def exists(self, checksum: str) -> bool:
        """
        return True if checksum is in local cache

        :param checksum <str>: hexdigest of checksum
        :return <bool>: True if checksum also known
        """
        if checksum in self.cache:  # if in cache, ok
            return True
        return self._exists(checksum)

    def purge_cache(self):
        """
        delete locally cached checksums
        """
        self._logger.info(
            f"deleting local cached checksum database in file {self._cache_filename}"
        )
        del self.checksums  # to close database and release file
        os.unlink(self._cache_filename)
        self.checksums = Checksums(self._cache_filename)
