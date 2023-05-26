#!/usr/bin/python3
import argparse
import datetime
import logging
import time
import socket
import json
import tarfile
import os

from webstorageS3 import FileStorageClient, WebStorageArchiveClient

logging.basicConfig(level=logging.INFO)

# according to platform search for config file in home directory
if os.name == "nt":
    HOMEPATH = os.path.join(os.path.expanduser("~"), "AppData", "Local", "webstorage")
else:
    HOMEPATH = os.path.join(os.path.expanduser("~"), ".webstorage")


def main():

    archive = {
        "path": None,
        "filedata": {},  # dict of file informations
        "blacklist": None,
        "starttime": time.time(),
        "stoptime": None,
        "hostname": socket.gethostname(),
        "tag": args.tag,
        "datetime": datetime.datetime.today().isoformat(),
    }

    with open(0, "rb") as infile:
        with tarfile.open(fileobj=infile, mode="r|") as tar:
            while True:
                info = tar.next()
                if not info:  # break if no other file left
                    break
                if info.isfile():
                    reader = tar.extractfile(info)
                    if reader:  # ioBuffer
                        metadata = fsc.put(reader)
                        if metadata["filehash_exists"] is True:
                            action_str = "FDEDUP"
                        else:
                            if metadata["blockhash_exists"] > 0:
                                action_str = "BDEDUP"
                            else:
                                action_str = "PUT"
                        logging.info(f"{action_str:8} {info.mtime} {info.name}")
                        archive["filedata"][info.name] = {
                            "checksum": metadata["checksum"],
                            "stat": (
                                info.mtime,
                                info.mtime,
                                info.mtime,
                                info.uid,
                                info.gid,
                                info.mode,
                                info.size,
                            ),  # in order like os.stats
                            "filetype": int(info.type),
                        }
                else:  # non file types, like subdir, device, etc.
                    logging.info(f"{'ADD':8} {info.mtime} {info.name}")
                    archive["filedata"][info.name] = {
                        "checksum": None,
                        "stat": (
                            info.mtime,
                            info.mtime,
                            info.mtime,
                            info.uid,
                            info.gid,
                            info.mode,
                            info.size,
                        ),  # in order like os.stats
                        "filetype": int(info.type),
                    }

    if archive["filedata"]:
        archive["stoptime"] = time.time()
        logging.debug(json.dumps(archive, indent=2))
        wsa.save(archive)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        "Tool to store streamed tar archives to WebStorageS3"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        default=False,
        action="store_true",
        help="turn on verbose output",
    )
    parser.add_argument(
        "-q", "--quiet", default=False, action="store_true", help="turn on quiet mode"
    )
    parser.add_argument("tag", default="tarfile", help="tag to name this archive")
    parser.add_argument(
        "--backend", default="DEFAULT", help="WebStorageS3 backend to use"
    )
    parser.add_argument(
        "--homepath", default=HOMEPATH, help="path to config and cache directory"
    )
    args = parser.parse_args()

    # set logging level
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    elif args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    else:
        logging.getLogger("").setLevel(logging.INFO)

    logging.debug(args)

    # checking homepath
    if not os.path.isdir(args.homepath):
        logging.error(
            f"first create directory {args.homepath} and place webstorage.yml file in there"
        )
        sys.exit(1)

    try:

        fsc = FileStorageClient(s3_backend=args.backend)
        wsa = WebStorageArchiveClient(s3_backend=args.backend)
        main()

    except KeyboardInterrupt:
        pass
