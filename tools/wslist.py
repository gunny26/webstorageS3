#!/usr/bin/python3
import os
import gzip
import json
import sys
import argparse
# own modules
import webstorageS3


parser = argparse.ArgumentParser("listing available wstar archives, and showing content (-l)")
parser.add_argument("-l", "--list", help="sow content of names archive")
parser.add_argument("-f", "--file", help="instead of using WebStorageArchive, read from file")
parser.add_argument("-v", "--verbose", action="store_true", help="more verbose, showing all files")
parser.add_argument("-c", "--cache", action="store_true", help="use only local cache to check if checksum is available")
args = parser.parse_args()
wa =webstorageS3.WebStorageArchiveClient()
try:
    if args.list is args.file is None:
        for entry in wa.get_backupsets():
            # something like
            # {'date': '2020-10-03', 'time': '16:27:23', 'datetime': '2020-10-03T16:27:23.106443', 'size': 5867399, 'tag': 'mesznera', 'hostname': 'ws00007999', 'basename': 'f5092d18a1e7f15a510a2d43ffe6a3a5bf12fca37b01366d3d50da0ec0da76d8'}
            print(f"{entry['datetime']} {entry['basename']} {entry['size']} {entry['hostname']} {entry['tag']}")
    else:
        if args.file:
            print(f"reading from file {args.file}")
            with gzip.open(args.file, "rt") as infile:
                data = json.loads(infile.read())
        else:
            print(f"geting archive with checksum {args.list} from WebStorageArchive backend")
            data = wa.read(args.list)
        print(f"wstar done on hostname    : {data['hostname']}")
        print(f"wstar used with path      : {data['path']}")
        print(f"wstar has checksum        : {data['checksum']}")
        print(f"wstar done at datestring  : {data['datetime']}")
        print(f"number of files in archive: {data['totalcount']}")
        print(f"total size of archive     : {data['totalsize']}")
        for filename, entry in data["filedata"].items():
            print(f"{entry['checksum']} {entry['stat'][6]:12} {filename}")
except KeyboardInterrupt:
    print("existing by user interruption")
    sys.exit(1)
