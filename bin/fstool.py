#!/usr/bin/python3
import argparse
import logging
import os
import sys
import yaml

logging.basicConfig(level=logging.INFO, format="%(message)s")

# own modules
from webstorageS3 import FileStorageClient


HOMEPATH = webstorageS3.HOMEPATH
# # according to platform search for config file in home directory
# if os.name == "nt":
#     HOMEPATH = os.path.join(os.path.expanduser("~"), "AppData", "Local", "webstorage")
# else:
#     HOMEPATH = os.path.join(os.path.expanduser("~"), ".webstorage")


def main():

    if args.infile:
        client = FileStorageClient(
            homepath=args.homepath, cache=args.cache, s3_backend=args.backend
        )

        with open(args.infile, "rb") as infile:
            recipe = client.put(infile)
            logging.info(f"file {args.infile} sucessfully stored in FileStorage")
            logging.info(yaml.dump(recipe, indent=2))
            logging.info(
                f"you can download this file with checksum {recipe['checksum']} from FileStorage"
            )
            logging.info(f"example: fstool.py {recipe['checksum']} --outfile <outputfilename>")

    if args.arguments:
        client = FileStorageClient(
            homepath=args.homepath, cache=args.cache, s3_backend=args.backend
        )

        if args.outfile:
            assert len(args.arguments) == 1
            if os.path.exists(args.outfile):
                logging.error(f"--outfile {args.outfile} already exists")
                return
            with open(args.outfile, "ab") as outfile:
                size = 0
                logging.info(f"writing file with checksum {args.arguments[0]} to {args.outfile}")
                recipe = client.get(args.arguments[0])
                logging.info(yaml.dump(recipe, indent=2))
                for block in client.read(args.arguments[0]):
                    outfile.write(block)
                    size += len(block)
                outfile.flush()
                logging.info(f"finished, wrote {size} to {args.outfile}")
        else:
            for checksum in args.arguments:
                data = client.get(checksum)
                logging.info("-" * 80)
                logging.info("block information about this filehash")
                logging.info("-" * 80)
                logging.info(yaml.dump(data, indent=2))
                logging.info("-" * 80)
                logging.info("meta informations about object")
                logging.info("-" * 80)
                logging.info(yaml.dump(client.head(checksum), indent=2))

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
    parser.add_argument(
        "--homepath",
        default=HOMEPATH,
        help="path to config directory and place for cache",
    )
    parser.add_argument(
        "--backend", default="DEFAULT", help="select backend in config file"
    )
    parser.add_argument(
        "--nocache",
        default=False,
        action="store_true",
        help="disable persistent checksum cache database",
    )
    parser.add_argument(
        "--outfile",
        help="output data to this filename, otherwise only meta data will be shown",
    )
    parser.add_argument(
        "--infile",
        help="file to read and store content in FileStorage",
    )
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
    parser.add_argument(
        "arguments", nargs="*", help="number of checsums of data blocks"
    )
    args = parser.parse_args()
    args.cache = not args.nocache

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
