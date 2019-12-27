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
    if len(sys.argv) != 2:
        print("ERROR: usage: fput <filename>")
        sys.exit(1)
    if not os.path.isfile(sys.argv[1]):
        print(f"ERROR: file {sys.argv[1]}  not found")
        sys.exit(2)
    with open(sys.argv[1], "rb") as infile:
        fs = FileStorageClient(cache=False)
        recipe = fs.put(infile)
        print(f"file {sys.argv[1]} stored in FileStorage")
        print(json.dumps(recipe, indent=2))
        print(f"you can download this file with checksum {recipe['checksum']} from FileStorage")
        print(f"example: fget.py {recipe['checksum']} <outputfilename>")

if __name__ == "__main__":
    main()
