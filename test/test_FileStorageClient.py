#!/usr/bin/python3
import hashlib
import json
import logging
logging.basicConfig(level=logging.INFO)
# own modules
from webstorageS3 import FileStorageClient

FS = FileStorageClient()

class Test(unittest.TestCase):

    def setUp(self):
        self.fs = FS

    def test_checksums(self):
        """
        get available checksums
        """
        print("Test checksums:")
        checksums = self.fs.checksums
        print(f"found {len(checksums)} in FileStorage")

    def test_put(self):
        """
        put a file onto Filestorage
        """
        print("Test PUT:")
        with open("../testdata/block.bin", "rb") as infile:
            metadata = self.fs.put(infile)
            print("received metadata from put:")
            print(json.dumps(metadata, indent=2))

    def test_get(self):
        """
        getting some files from FileStorage
        """
        print("Test GET verified:")
        limit = 10
        for filechecksum in self.fs.checksums:
            metadata = self.fs.get(filechecksum)
            print(json.dumps(metadata, indent=4))
            print("file {filechecksum} consists of {len(metadata['blockchain'])} blocks")
            digest = hashlib.sha1()
            size = 0
            localdata = bytes()
            if len(metadata["blockchain"]) < 10:
                for data in self.fs.read(filechecksum):
                    localdata += data
                    print("received block with length ", len(data))
                    digest.update(data)
                    size += len(data)
            print("calculated digest ", digest.hexdigest())
            print("calculated size ", size)
            assert digest.hexdigest() == filechecksum
            assert size == metadata["size"]
            if limit == 0:
                break
            limit -= 1


if __name__ == "__main__":
    unittest.main()
