#!/usr/bin/python
import sys
import os
import time
import json
import random
import hashlib
import unittest
from io import BytesIO
import logging
logging.basicConfig(level=logging.INFO)
#logging.getLogger("requests").setLevel(logging.WARNING)
#logging.getLogger("urllib3").setLevel(logging.WARNING)
from BlockStorageClientS3 import BlockStorageClient
from BlockStorageClientS3 import BlockStorageError

BS = BlockStorageClient()

teststring = "A" * 1024
testblock = teststring.encode("ascii")
sha1 = hashlib.sha1()
sha1.update(testblock)
testchecksum = sha1.hexdigest()
data = BytesIO(testblock)
print(data.read().hex())


class Test(unittest.TestCase):

    def setUp(self):
        self.bs = BS

    def test_put(self):
        print("PUT-ing:")
        print(testblock.hex())
        checksum, status = self.bs.put(testblock)
        data1 = self.bs.get(testchecksum)
        self.assertTrue(testblock == data1)

    def test_put_too_long(self):
        data = "A" * 1025 * 1024
        data = data.encode("ascii")
        print(len(data))
        try:
            checksum, status = self.bs.put(data)
        except BlockStorageError as exc:
            print(exc)

    def test_get_verify(self):
        print("PUT-ing:")
        print(testblock.hex())
        checksum, status = self.bs.put(testblock)
        data1 = self.bs.get_verify(checksum)
        self.assertTrue(testblock == data1)

    def test_get_exists(self):
        checksum, status = self.bs.put(testblock)
        self.assertTrue(self.bs.exists(checksum))
        self.assertFalse(self.bs.exists("0" * 40)) # illegal checksum


if __name__ == "__main__":
    unittest.main()
