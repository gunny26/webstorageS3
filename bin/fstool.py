#!/usr/bin/python3
import hashlib
import argparse
import logging
# own modules
from webstorageS3 import FileStorageClient

def verify_checksum(checksum):
    data = client.get(checksum)
    if data["checksum"] == checksum:
        print(f"OK: file checksum {checksum} matching remote checksum {data['checksum']}, contains {len(data['blockchain'])} blocks, size {data['size']}")

def verify_all():
    for checksum in client.checksums:
        verify_checksum(checksum)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FileStorageClient Tool")
    parser.add_argument("--verify", dest="verify", action="store_true", help="verify checksums")
    parser.add_argument("-q", "--quiet", action="store_true", help="switch to loglevel ERROR")
    parser.add_argument("-v", "--verbose", action="store_true", help="switch to loglevel DEBUG")
    args = parser.parse_args()
    # set logging level
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    client = FileStorageClient()
    if args.verify:
        verify_all()
