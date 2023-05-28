#!/usr/bin/python3
import os
import gzip
import json
import sys
import argparse
# own modules
import webstorageS3


parser = argparse.ArgumentParser("verifying wstar archives, checking if stored files are in FileStorage")
parser.add_argument("filename", help="wstar filename to verify")
parser.add_argument("-v", "--verbose", action="store_true", help="more verbose, showing all files")
parser.add_argument("--nocache", action="store_true", help="use only local cache to check if checksum is available")
args = parser.parse_args()
try:
    if not os.path.isfile(args.filename):
        print(f"file {args.filename} does not exist")
        sys.exit(1)
    with gzip.open(args.filename, "rb") as infile:
        data = json.loads(infile.read())
        print(f"wstar done on hostname    : {data['hostname']}")
        print(f"wstar used with path      : {data['path']}")
        print(f"wstar has checksum        : {data['checksum']}")
        print(f"wstar done at datestring  : {data['datetime']}")
        print(f"number of files in archive: {data['totalcount']}")
        print(f"total size of archive     : {data['totalsize']}")
        if args.verbose:
            print("showing all files in wstar")
        else:
            print("showing only missing checksums, otherwise use -v to show all files")
        if args.nocache:
            print("forcing s3 object check, slower but more secure, otherwise use --nocache to use only local cache")
        else:
            print("checking aganinst local cache database only")
        fs =webstorageS3.FileStorageClient(cache=not args.nocache)
        for filename, entry in data["filedata"].items():
            if not fs.exist(entry['checksum'], force=True) or args.verbose:
                print(f"{entry['checksum']} {entry['stat'][6]:12} {filename}")
except KeyboardInterrupt:
    print("existing by user interruption")
    sys.exit(1)
