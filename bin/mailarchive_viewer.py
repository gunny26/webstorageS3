#!/usr/bin/python3
import sys
import hashlib
import json
import os
import logging
# logging.basicConfig(level=logging.INFO)
# own modules
from webstorageS3 import FileStorageClient


def main():
    # read information from fstor datastructure
    # and get according file data stream to sys.stdout
    if len(sys.argv) != 2:
        print("usage: mailarchive <path to .fstor file")
        sys.exit(1)
    fs = FileStorageClient(cache=False)
    metadata = json.loads(open(sys.argv[1], "rt").read())
    # print(metadata)
    print(f"downloading filestore object with checksum {metadata['checksum']}")
    recipe = fs.get(metadata["checksum"]) # that should be the same as locally stored
    #print(recipe)
    mail_b = bytes()
    for checksum in recipe["blockchain"]:
        mail_b += fs.blockstorage.get(checksum)
    mail = json.loads(mail_b.decode("utf-8"))
    #print(json.dumps(mail, indent=2))
    print(f"received on {mail['mail']['received']} with size {mail['mail']['size']}")
    print(f"From   : {mail['mail']['from']}")
    print(f"To     : {mail['mail']['to']}")
    print(f"Cc     : {mail['mail']['cc']}")
    print(f"Subject: {mail['mail']['subject']}")
    print(f"{'-' * 80}")
    print(mail["body"].replace("\r\n\r\n", "\n"))
    print("Attachments to this mail:")
    print("-------------------------")
    for attachment in mail["mail"]["attachments"]:
        print(attachment[1], "\t", attachment[0])

if __name__ == "__main__":
    main()
