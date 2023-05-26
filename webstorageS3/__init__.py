#!/usr/bin/python3
import os

# according to platform search for config file in home directory
if os.name == "nt":
    HOMEPATH = os.path.join(os.path.expanduser("~"), "AppData", "Local", "webstorage")
else:
    HOMEPATH = os.path.join(os.path.expanduser("~"), ".webstorage")

from .BlockStorageClientS3 import BlockStorageClient, BlockStorageError
from .Checksums import Checksums
from .FileStorageClientS3 import FileStorageClient
from .WebStorageArchiveClientS3 import WebStorageArchiveClient
