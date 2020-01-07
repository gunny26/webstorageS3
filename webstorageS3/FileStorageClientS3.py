#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use FileStorage WebApp
"""
import os
import json
import logging
#import hashlib
from io import BytesIO
# own modules
from .StorageClientS3 import StorageClient
from .BlockStorageClientS3 import BlockStorageClient
from .Checksums import Checksums

class FileStorageClient(StorageClient):
    """
    put some arbitrary file like data object into BlockStorage and remember how to reassemble it
    the recipe to reassemble will be stored in FileStorage
    """

    def __init__(self, cache=True):
        """__init__"""
        super(FileStorageClient, self).__init__()
        self._logger = logging.getLogger(self.__class__.__name__)
        self._bs = BlockStorageClient(cache=cache)
        self._logger.debug("bucket list: %s", self._client.list_buckets())
        self._bucket_name = self._config["FILESTORAGE_BUCKET_NAME"]
        self._checksums = Checksums(os.path.join(self._homepath, "_filestorage_cache.db"))
        self._logger.info("found %d stored checksums in local cache", len(self._checksums))
        self._get_checksums()
        self._logger.info("found %d stored checksums after cache and bucket", len(self._checksums))

    @property
    def blockstorage(self):
        """
        reference to used BlockStorage
        """
        return self._bs # TODO: is this necessary

    def put(self, fh, mime_type="application/octet-stream"):
        """
        save data of fileobject in Blockstorage

        data is read in blocks
        every block will be checksummed and tested if exists against
        BlockStorage
          if not existing, put it into BlockStorage
        the whole file is also checksummed and tested against FileStorage
          if not existing, put it into FileStorage

        :param fh <filehandle>: to read data from in binary mode
        :param mime_type <str>: defaults to application/octet-stream if not given
        """
        metadata = {
            "blockchain" : [],
            "size" : 0,
            "checksum" : None,
            "mime_type" : mime_type,
            "filehash_exists" : False, # indicate if the filehash already
            "blockhash_exists" : 0, # how many blocks existed already
        }
        filehash = self._hashfunc()
        # Put blocks in Blockstorage
        data = fh.read(self._bs.blocksize)
        while data:
            metadata["size"] += len(data)
            filehash.update(data) # running filehash until end
            checksum, status = self._bs.put(data, use_cache=True)
            self._logger.debug("PUT blockcount: %d, checksum: %s, status: %s", len(metadata["blockchain"]), checksum, status)
            # 202 - skipped, block in cache, 201 - rewritten, block existed
            if status in (201, 202):
                metadata["blockhash_exists"] += 1
            metadata["blockchain"].append(checksum)
            data = fh.read(self._bs.blocksize)
        self._logger.debug("put %d blocks in BlockStorage, %d existed already", len(metadata["blockchain"]), metadata["blockhash_exists"])
        # put file composition into filestorage
        filedigest = filehash.hexdigest()
        metadata["checksum"] = filedigest
        if filedigest not in self._checksums: # check if filehash is already stored
            self._logger.debug("storing recipe for filechecksum: %s", filedigest)
            self._put(filedigest, metadata)
            return metadata
        self._logger.debug("filehash %s already stored", filedigest)
        metadata["filehash_exists"] = True
        return metadata

    def _put(self, checksum, data):
        """
        put some arbitrary data into storage
        :param checksum <str>: hexdigest of checksum
        :param data <dict>: meta data to this checksum
        """
        if checksum in self._checksums:
            self._logger.debug("202 - skip this block, checksum is in list of cached checksums")
            return checksum, 202
        self._client.upload_fileobj(BytesIO(json.dumps(data).encode("utf-8")), self._bucket_name, checksum) # TODO: exceptions
        self._checksums.add(checksum) # add to local cache
        return checksum, 200 # fake

    def read(self, checksum):
        """
        return data as generator
        yields data blocks of self.blocksize
        the last block is almost all times less than self.blocksize
        :param checksum <str>: hexdigest of checksum
        """
        for block in self.get(checksum)["blockchain"]:
            yield self._bs.get(block)

    def get(self, checksum):
        """
        returns blockchain of file defined by hexdigest

        this is not the data of this file, only the plan how to assemble the file

        :param checksum <str>: hexdigest of checksum
        """
        b_buffer = BytesIO()
        self._client.download_fileobj(self._bucket_name, checksum, b_buffer) # TODO: exceptions
        b_buffer.seek(0) # do not forget this tiny little line !!
        data = b_buffer.read().decode("utf-8")
        return json.loads(data)

