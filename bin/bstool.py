#!/usr/bin/python3
import hashlib
import argparse
import logging
# own modules
from webstorageS3 import BlockStorageClient

def verify_checksum(checksum):
    data = client.get(checksum)
    sha1 = hashlib.sha1()
    sha1.update(data)
    if len(data) > client.blocksize:
        print(f"ERROR: length mismatch of remote {checksum} and locally {sha1.hexdigest()} size {len(data)}")
    else:
        if sha1.hexdigest() != checksum:
            print(f"ERROR: checksum mismatch between remote {checksum} and locally {sha1.hexdigest()} size {len(data)}")
        else:
            print(f"OK: checksum match between remote {checksum} and locally {sha1.hexdigest()} size {len(data)}")

def verify_all():
    for checksum in client.checksums:
        verify_checksum(checksum)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlockStorageClient Tool")
    parser.add_argument("--verify", dest="verify", action="store_true", help="verify checksums")
    parser.add_argument("-q", "--quiet", action="store_true", help="switch to loglevel ERROR")
    parser.add_argument("-v", "--verbose", action="store_true", help="switch to loglevel DEBUG")
    args = parser.parse_args()
    # set logging level
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    client = BlockStorageClient()
    if args.verify:
        verify_all()
