#!/usr/bin/python3
import hashlib
import json
import logging
logging.basicConfig(level=logging.INFO)
# own modules
from FileStorageClientS3 import FileStorageClient


if __name__ == "__main__":
    fs = FileStorageClient()
    print("checking all existing files in filestorage")
    checksums = fs.checksums
    print("found %d file checksums" % len(checksums))
    with open("test.dbm", "rb") as infile:
        fs.put(infile)
    for filechecksum in checksums:
        metadata = fs.get(filechecksum)
        print(json.dumps(metadata, indent=4))
        print("file %s consists of %d blocks" % (filechecksum, len(metadata["blockchain"])))
        digest = hashlib.sha1()
        size = 0
        localdata = bytes()
        if len(metadata["blockchain"]) < 10:
            for data in fs.read(filechecksum):
                localdata += data
                print("received block with length ", len(data))
                digest.update(data)
                size += len(data)
        print("calculated digest ", digest.hexdigest())
        print("calculated size ", size)
        assert digest.hexdigest() == filechecksum
        assert size == metadata["size"]
    print("put some data in filestorage")
    res = fs.put(open("test.dbm", "rb"))
    print(res)
