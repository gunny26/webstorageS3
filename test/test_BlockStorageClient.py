#!/usr/bin/python
import hashlib
import unittest
from io import BytesIO
import logging
logging.basicConfig(level=logging.INFO)
# own modules
from BlockStorageClientS3 import BlockStorageClient, BlockStorageError

BS = BlockStorageClient()

teststring = "A" * BS.blocksize
testblock = teststring.encode("ascii")
sha1 = hashlib.sha1() # must be the same as defined for Blockstorage
sha1.update(testblock)
testchecksum = sha1.hexdigest()
print(f"created valid testblock with length {len(testblock)}")
print(f"testblock has checksum {testchecksum}")
data = BytesIO(testblock)


class Test(unittest.TestCase):

    def setUp(self):
        self.bs = BS

    def test_put(self):
        """
        putting some testblock to blockstorage
        """
        print("Test PUT:")
        checksum, status = self.bs.put(testblock)
        data1 = self.bs.get(testchecksum)
        self.assertTrue(testblock == data1)

    def test_put_too_long(self):
        """
        put a block bigger than max blocksize to BlockStorage, expecting Exception
        """
        print("Test PUT too big:")
        data = "A" * (self.bs.blocksize + 1)
        data = data.encode("ascii")
        print(f"generated some block bigger than max blocksize, with length {len(data)}")
        try:
            checksum, status = self.bs.put(data)
        except BlockStorageError as exc:
            print(exc)

    def test_get_verify(self):
        """
        getting some block from Blockstorage, verifying after downloading
        """
        print("Test GET verified:")
        checksum, status = self.bs.put(testblock)
        data1 = self.bs.get_verify(checksum)
        self.assertTrue(testblock == data1)

    def test_get_exists(self):
        """
        checkick if some checksum exists
        """
        print("Test Exists:")
        checksum, status = self.bs.put(testblock)
        self.assertTrue(self.bs.exists(checksum))
        self.assertFalse(self.bs.exists("0" * 40)) # illegal checksum


if __name__ == "__main__":
    unittest.main()
