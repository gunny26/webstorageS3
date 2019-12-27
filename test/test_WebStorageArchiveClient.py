#!/usr/bin/python3
import random
import hashlib
import json
import socket
import unittest
import logging
logging.basicConfig(level=logging.INFO)
# own modules
from WebStorageArchiveClientS3 import WebStorageArchiveClient

WSAC = WebStorageArchiveClient()

class Test(unittest.TestCase):

    def setUp(self):
        self.wsac = WSAC

    def test_backupsets(self):
        """
        get available backupsets
        """
        print("Test backupsets:")
        backupsets = self.wsac.get_backupsets()
        print(f"found {len(backupsets)} in WebStorageArchive")
        if backupsets:
            print(f"content of first backupset")
            print(json.dumps(backupsets[0], indent=2))

    def test_latest_backupset(self):
        """
        get latest backupset
        """
        print("Test latest:")
        latest_backupset = self.wsac.get_latest_backupset()
        print("latest backupset found: ", json.dumps(latest_backupset, indent=2))

    def test_read(self):
        latest_backupset = self.wsac.get_latest_backupset()
        if latest_backupset:
            print("getting latest backupset")
            data = self.wsac.read(latest_backupset)
            print(json.dumps(data, indent=2))



if __name__ == "__main__":
    unittest.main()
