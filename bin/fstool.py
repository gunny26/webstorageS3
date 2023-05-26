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


def main():

    if args.verify_all:

        client = FileStorageClient(homepath=args.homepath, s3_backend=args.backend)
        for checksum in client.checksums:
            data = client.get(checksum)
            if data["checksum"] == checksum:
                logging.info(
                    f"OK: file checksum {checksum} matching remote checksum {data['checksum']}, contains {len(data['blockchain'])} blocks, size {data['size']}"
                )

    if args.copy:

        client = FileStorageClient(
            homepath=args.homepath, s3_backend=args.backend, cache=False
        )
        client_target = FileStorageClient(
            homepath=args.homepath, s3_backend=args.target_backend, cache=True
        )
        for checksum in client.checksums:

            data = client.get(checksum)
            logging.info(
                f"copy file with checksum {checksum} containing {len(data['blockchain'])} blocks, size {data['size']}"
            )

            blocks = len(data["blockchain"])
            for block_index, block_checksum in enumerate(data["blockchain"]):
                block_data = client.blockstorage.get(block_checksum)
                client_target.blockstorage.put(block_data)
                logging.info(
                    f"  {block_index}/{blocks} copy block {block_checksum} size {len(block_data)}"
                )

            client_target._put(checksum, data)

    if args.sync:

        client = FileStorageClient(
            homepath=args.homepath, s3_backend=args.backend, cache=True
        )
        client_target = FileStorageClient(
            homepath=args.homepath, s3_backend=args.target_backend, cache=True
        )
        number_checksums = len(client.cache)
        for index, checksum in enumerate(client.cache):
            if checksum in client_target.cache:
                continue

            data = client.get(checksum)
            logging.info(
                f"{index}/{number_checksums} copy file with checksum {checksum} containing {len(data['blockchain'])} blocks, size {data['size']}"
            )

            blocks = len(data["blockchain"])
            for block_index, block_checksum in enumerate(data["blockchain"]):
                block_data = client.blockstorage.get(block_checksum)
                client_target.blockstorage.put(block_data)
                logging.info(
                    f"  {block_index}/{blocks} copy block {block_checksum} size {len(block_data)}"
                )

            client_target._put(checksum, data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FileStorageClient Tool")
    parser.add_argument(
        "-q", "--quiet", action="store_true", help="switch to loglevel ERROR"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="switch to loglevel DEBUG"
    )
    parser.add_argument("--homepath", default=HOMEPATH, help="path to config directory")
    parser.add_argument("--backend", default="DEFAULT", help="select s3 backend")
    parser.add_argument(
        "--target-backend", help="target s3 backend for copy and sync operations"
    )
    parser.add_argument(
        "--copy",
        action="store_true",
        help="copy data from backend to target-backend <LONG OPERATION>",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="copy cache data from backend to target-backend",
    )
    parser.add_argument(
        "--verify-all", action="store_true", help="verify checksums <LONG OPERATION>"
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

        main()

    except KeyboardInterrupt:
        pass
