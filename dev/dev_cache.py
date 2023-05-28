#!/usr/bin/python3
import json
# own modules
from webstorageS3 import FileStorageClient, BlockStorageClient

fs = FileStorageClient()
with open("fs_cache.json", "wt") as outfile:
    outfile.write(json.dumps(fs.checksums))

