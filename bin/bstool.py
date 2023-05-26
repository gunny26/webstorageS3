#!/usr/bin/python3
import hashlib
import argparse
import yaml
import logging
import os
import sys

# own modules
from webstorageS3 import BlockStorageClient

logging.basicConfig(level=logging.INFO)

# according to platform search for config file in home directory
if os.name == "nt":
    HOMEPATH = os.path.join(os.path.expanduser("~"), "AppData", "Local", "webstorage")
else:
    HOMEPATH = os.path.join(os.path.expanduser("~"), ".webstorage")


def sizeof_fmt(num, suffix="B"):
    """
    function to convert numerical size number into human readable number
    taken from https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    """
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


def main():

    if args.arguments:
        logging.debug(args.arguments)
        client = BlockStorageClient(
            homepath=args.homepath, cache=args.cache, s3_backend=args.backend
        )
        if args.out:
            with os.fdopen(sys.stdout.fileno(), "ab", closefd=False) as stdout:
                for checksum in args.arguments:
                    stdout.write(client.get(checksum))
                stdout.flush()
        else:
            for checksum in args.arguments:
                data = client.get(checksum)
                sha1 = hashlib.sha1()
                sha1.update(data)
                sha256 = hashlib.sha256()
                sha256.update(data)
                md5 = hashlib.md5()
                md5.update(data)

                print("-" * 80)
                print("block checksum informations")
                print("-" * 80)
                print(f"MD5    checksum {md5.hexdigest()}")
                print(f"SHA1   checksum {sha1.hexdigest()}")
                print(f"SHA256 checksum {sha256.hexdigest()}")

                if len(data) > client.blocksize:
                    logging.error(
                        f"ERROR: length mismatch of remote {checksum} and locally {sha1.hexdigest()} size {len(data)}"
                    )
                else:
                    if sha1.hexdigest() != checksum:
                        logging.error(
                            f"ERROR: checksum mismatch between remote {checksum} and locally {sha1.hexdigest()} size {len(data)}"
                        )
                    else:
                        logging.info(
                            f"OK: checksum match between remote {checksum} and locally {sha1.hexdigest()} size of data block {len(data)}"
                        )

                print("-" * 80)
                print("meta informations about object")
                print("-" * 80)
                print(yaml.dump(client.head(checksum), indent=2))

                # some hexdump like output
                if args.hexdump:
                    print("-" * 80)
                    print("hexdump of data")
                    print("-" * 80)
                    colnum = 30
                    col = 0
                    hex_part = []
                    ascii_part = []
                    for item in data:
                        if col < colnum:
                            hex_part.append("%02x" % item)
                            if 31 < item < 127:
                                ascii_part.append(chr(item))
                            else:
                                ascii_part.append(".")
                            col += 1
                        else:
                            print(f"{' '.join(hex_part)}    {''.join(ascii_part)}")
                            ascii_part = []
                            hex_part = []
                            col = 0

    if args.copy:

        client_source = BlockStorageClient(
            homepath=args.homepath, cache=False, s3_backend=args.backend
        )
        client_target = BlockStorageClient(
            homepath=args.homepath, cache=True, s3_backend=args.target_backend
        )
        for checksum in client_source.checksums:
            logging.info(
                f"copy {checksum} from {args.backend} to {args.target_backend}"
            )
            data = client_source.get(checksum)
            sha1 = hashlib.sha1()
            sha1.update(data)
            if sha1.hexdigest() == checksum:
                res_checksum, _ = client_target.put(data, use_cache=True)
                if res_checksum != checksum:
                    logging.error(f"error storing data with checksum {checksum}")
            else:
                logging.error(f"checksum mismatch at checksum {checksum}, skiping copy")

    if args.sync:

        client_source = BlockStorageClient(
            homepath=args.homepath, cache=True, s3_backend=args.backend
        )
        client_target = BlockStorageClient(
            homepath=args.homepath, cache=True, s3_backend=args.target_backend
        )
        num_checksums = len(client_source.cache)
        for index, checksum in enumerate(
            client_source.cache
        ):  # thats the difference to copy
            if checksum in client_target.cache:
                continue
            logging.info(
                f"{index}/{num_checksums} copy {checksum} from {args.backend} to {args.target_backend}"
            )
            data = client_source.get(checksum)
            sha1 = hashlib.sha1()
            sha1.update(data)
            if sha1.hexdigest() == checksum:
                res_checksum, _ = client_target.put(data, use_cache=True)
                if res_checksum != checksum:
                    logging.error(f"error storing data with checksum {checksum}")
            else:
                logging.error(f"checksum mismatch at checksum {checksum}, skiping copy")

    if args.verify_all:

        client = BlockStorageClient(
            homepath=args.homepath, cache=args.cache, s3_backend=args.backend
        )
        logging.info("checking all stored checksum, this could take some time")
        for checksum in client.checksums:
            data = client.get(checksum)
            sha1 = hashlib.sha1()
            sha1.update(data)
            if sha1.hexdigest() != checksum:
                logging.error(
                    f"ERROR: checksum mismatch between remote {checksum} and locally {sha1.hexdigest()} size {len(data)}"
                )
            else:
                logging.info(
                    f"OK: checksum match between remote {checksum} and locally {sha1.hexdigest()} size {len(data)}"
                )

    if args.list:
        client = BlockStorageClient(
            homepath=args.homepath, cache=args.cache, s3_backend=args.backend
        )
        logging.info("checking all stored checksum, this could take some time")
        # {
        #   'Key': '00b8abb14057692f7b6b272af1406b340392454c',
        #   'LastModified': datetime.datetime(2023, 4, 5, 12, 18, 7, tzinfo=tzutc()),
        #   'ETag': '"2489cb415910191739b6633dc1e3e287"',
        #   'Size': 1048576,
        #   'StorageClass': 'STANDARD',
        #   'Owner': {'ID': 'c637bcf892367c407abbbe39c4ee9a949f384286f8873b81f82dcda07185f7b1'}
        # }
        for checksum in client.list():
            print(f"{checksum['LastModified']} {sizeof_fmt(checksum['Size'])}\t{checksum['Key']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="BlockStorageClient Tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
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
        "--verify-all",
        action="store_true",
        help="verify all stored checksums <LONG OPERATION>",
    )
    parser.add_argument(
        "-o",
        dest="out",
        action="store_true",
        help="output data to stdout, otherwise only meta data will be shown",
    )
    transfer_parser = parser.add_argument_group('transfer')
    transfer_parser.add_argument(
        "--copy",
        action="store_true",
        help="copy every block from source to target bucket <LONG OPERATION>",
    )
    transfer_parser.add_argument(
        "--sync",
        action="store_true",
        help="copy cached blocks from source to target bucket",
    )
    transfer_parser.add_argument(
        "--target-backend", help="target bucket for copy or sync operation"
    )
    parser.add_argument(
        "--hexdump", action="store_true", default=False, help="show hexdump of data"
    )
    parser.add_argument(
        "--list", action="store_true", default=False, help="list blocks"
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

    if args.copy and not args.target_backend:
        logging.error("if using --copy you also have to provide --target-backend")
        sys.exit(1)

    if args.sync and not args.target_backend:
        logging.error("if using --sync you also have to provide --target-backend")
        sys.exit(1)

    try:

        main()

    except KeyboardInterrupt:
        pass
