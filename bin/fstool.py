#!/usr/bin/python3
import argparse
import logging
import os
import sys
# own modules
from webstorageS3 import FileStorageClient

logging.basicConfig(level=logging.INFO)

# according to platform search for config file in home directory
if os.name == "nt":
    HOMEPATH = os.path.join(os.path.expanduser("~"), "AppData", "Local", "webstorage")
else:
    HOMEPATH = os.path.join(os.path.expanduser("~"), ".webstorage")


def verify_checksum(checksum):
    data = client.get(checksum)
    if data["checksum"] == checksum:
        logging.info(f"OK: file checksum {checksum} matching remote checksum {data['checksum']}, contains {len(data['blockchain'])} blocks, size {data['size']}")


def verify_all():
    for checksum in client.checksums:
        verify_checksum(checksum)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FileStorageClient Tool")
    parser.add_argument("--verify", dest="verify", action="store_true", help="verify checksums")
    parser.add_argument("-q", "--quiet", action="store_true", help="switch to loglevel ERROR")
    parser.add_argument("-v", "--verbose", action="store_true", help="switch to loglevel DEBUG")
    parser.add_argument("--homepath", default=HOMEPATH, help="path to config directory")
    args = parser.parse_args()

    # set logging level
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)

    # checking homepath
    if not os.path.isdir(args.homepath):
        logging.error(f"first create directory {args.homepath} and place webstorage.yml file in there")
        sys.exit(1)

    client = FileStorageClient()
    if args.verify:
        verify_all()
