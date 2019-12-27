#!/usr/bin/python3
import sys
import hashlib
import json
import os
import logging
logging.basicConfig(level=logging.INFO)
# own modules
from FileStorageClientS3 import FileStorageClient


def main():
    # read information from fstor datastructure
    # and get according file data stream to sys.stdout
    if len(sys.argv) != 3:
        print("usage: fget <checksum> <outputfilename>")
        sys.exit(1)
    if os.path.exists(sys.argv[2]):
        print(f"ERROR: {sys.argv[2]} does already exist")
        sys.exit(2)
    fs = FileStorageClient(cache=False)
    with open(sys.argv[2], "wb") as outfile:
        print("downloading recipe with checksum {sys.argv[1]}")
        recipe = fs.get(sys.argv[1])
        print("downloading content for this checksum")
        print(json.dumps(recipe, indent=2))
        for block in fs.read(sys.argv[1]):
            outfile.write(block)
        print("done")

if __name__ == "__main__":
    main()
