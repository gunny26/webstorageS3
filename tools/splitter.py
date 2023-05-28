#!/usr/bin/python3
import json
import gzip
import os

for filename in os.listdir():
    if not os.path.isfile(filename):
        continue
    if len(filename) != 64:
        continue
    with gzip.open(filename, "rt") as infile:
        wstar = json.loads(infile.read())
        print(f"{wstar['datetime']} {wstar['totalsize']:10} {wstar['totalcount']:10} {wstar['hostname']}, {wstar['path']}")
        if not os.path.isdir(wstar['hostname']):
            os.mkdir(wstar['hostname'])
        outfilename = f"{wstar['hostname']}/{wstar['checksum']}"
        with gzip.open(outfilename, "wt") as outfile:
            outfile.write(json.dumps(wstar))
        os.utime(outfilename, (wstar["starttime"], wstar["stoptime"]))

