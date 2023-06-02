#!/usr/bin/python3
import os

from .blockstorage_client_s3 import BlockStorageClient, BlockStorageError
from .checksums import Checksums
from .filestorage_client_s3 import FileStorageClient
from .webstorage_archive_client_s3 import WebStorageArchiveClient
from .storageclient_s3 import StorageClient

# according to platform search for config file in home directory
if os.name == "nt":
    HOMEPATH = os.path.join(os.path.expanduser("~"), "AppData", "Local", "webstorage")
else:
    HOMEPATH = os.path.join(os.path.expanduser("~"), ".webstorage")


def sizeof_fmt(num, suffix="B"):
    """
    function to convert numerical size number into human readable number
    taken from https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    """
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"
