#!/usr/bin/python3
# pylint: disable=line-too-long
# disable=locally-disabled, multiple-statements, fixme, line-too-long
"""
command line program to create/restore/test WebStorageArchives
"""
import os
import datetime
import gzip
import json
import time
import sys
import socket
import argparse
import stat
import logging
# non std modules
import botocore
# own modules
from webstorageS3 import WebStorageArchiveClient, FileStorageClient


logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')


def filemode(st_mode):
    """
    convert stat st_mode number to human readable string
    taken from https://stackoverflow.com/questions/17809386/how-to-convert-a-stat-output-to-a-unix-permissions-string
    """
    is_dir = 'd' if stat.S_ISDIR(st_mode) else '-'
    dic = {'7': 'rwx', '6': 'rw-', '5': 'r-x', '4': 'r--', '0': '---'}
    perm = str(oct(st_mode)[-3:])
    return is_dir + ''.join(dic.get(x, x) for x in perm)


def sizeof_fmt(num, suffix='B'):
    """
    function to convert numerical size number into human readable number
    taken from https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    """
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def ppls(absfile, filedata):
    """
    pritty print ls
    return long filename format, like ls -al does
    for file statistics use filedate["stat"] segment
    """
    st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
    datestring = datetime.datetime.fromtimestamp(int(st_mtime))
    return "%10s %5s %5s %10s %19s %s" % (filemode(st_mode), st_uid, st_gid, sizeof_fmt(st_size), datestring, absfile)


def create_blacklist(absfilename):
    """
    generator for blacklist function

    read exclude file and generate blacklist function
    blacklist function returns True if filenae matches (re.match) any pattern
    """
    patterns = []
    logging.debug("reading exclude file")
    with open(absfilename, "rt") as exclude_file:
        for row in exclude_file:
            if len(row) <= 1:
                continue
            if row[0] == "#":
                continue
            operator = row.strip()[0]  # -/+
            pattern = row.strip()[2:]  # some string to use in match
            logging.debug("%s %s", operator, pattern)
            if operator == "-":
                patterns.append(pattern)

    def blacklist_func(filename):
        """
        returned closure to use for blacklist checking
        """
        logging.debug("matching %s", filename)
        return any((pattern in filename for pattern in patterns))

    return blacklist_func


def create(filestorage, path, blacklist_func, tag):
    """
    create a new archive of files under path
    filter out filepath which mathes some item in blacklist
    and write file to outfile in FileIndex

    filestorage ... <FileStorage> Object
    path ... <str> must be valid os path
    blacklist_func ... <func> called with absfilename, if True is returned, skip this file
    """
    archive_dict = {
        "path": path,
        "filedata": {},
        "blacklist": None,
        "starttime": time.time(),
        "stoptime": None,
        "hostname": socket.gethostname(),
        "tag": tag,
        "datetime": datetime.datetime.today().isoformat()
    }
    action_stat = {
        "PUT": 0,
        "FDEDUP": 0,
        "BDEDUP": 0,
        "EXCLUDE": 0,
    }
    action_str = "PUT"
    for root, dirs, files in os.walk(path):
        for filename in files:
            absfilename = os.path.join(root, filename)
            if blacklist_func(absfilename):
                logging.debug("EXCLUDE %s", absfilename)
                continue
            if not os.path.isfile(absfilename):
                # only save regular files
                continue
            try:
                stats = os.stat(absfilename)
                metadata = filestorage.put(open(absfilename, "rb"))
                if metadata["filehash_exists"] is True:
                    action_str = "FDEDUP"
                else:
                    if metadata["blockhash_exists"] > 0:
                        action_str = "BDEDUP"
                    else:
                        action_str = "PUT"
                archive_dict["filedata"][absfilename] = {
                    "checksum": metadata["checksum"],
                    "stat": (stats.st_mtime, stats.st_atime, stats.st_ctime, stats.st_uid, stats.st_gid, stats.st_mode, stats.st_size)
                }
                if action_str == "PUT":
                    logging.error("%8s %s", action_str, ppls(absfilename, archive_dict["filedata"][absfilename]))
                else:
                    logging.info("%8s %s", action_str, ppls(absfilename, archive_dict["filedata"][absfilename]))
                action_stat[action_str] += 1
            except (OSError, IOError, botocore.exceptions.ClientError) as exc:
                logging.error(f"error while processing file {absfilename}")
                logging.exception(exc)
    logging.info("file operations statistics:")
    for action, count in action_stat.items():
        logging.info("%8s : %s", action, count)
    archive_dict["stoptime"] = time.time()
    archive_dict["totalcount"] = len(archive_dict["filedata"])
    archive_dict["totalsize"] = sum((archive_dict["filedata"][absfilename]["stat"][-1] for absfilename in archive_dict["filedata"]))
    return archive_dict


def diff(filestorage, data, blacklist_func):
    """
    doing differential backup
    criteriat to check if some file is change will be the stats informations
    there is a slight possiblity, that the file has change by checksum but non in stats information

    filestorage ... <FileStorage> Object
    data ... <dict> existing data to compare with existing files
    blacklist_func ... <func> called with absfilename, if True is returned, skip this file
    """
    # check if some files are missing or have changed
    changed = False
    data["starttime"] = time.time()  # change to now
    data["datetime"] = datetime.datetime.today().isoformat()  # change to now
    for absfile in sorted(data["filedata"].keys()):
        filedata = data["filedata"][absfile]
        if os.path.isfile(absfile) is False:
            # remove informaion from data, if file was deleted
            logging.info("%8s %s", "DELETED", ppls(absfile, filedata))
            del data["filedata"][absfile]
            changed = True
        else:
            st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
            # check all except atime
            stats = os.stat(absfile)
            change = False
            # long version to print every single criteria
            if stats.st_mtime != st_mtime:
                logging.info("%8s %s", "MTIME", ppls(absfile, filedata))
                change = True
            elif stats.st_ctime != st_ctime:
                logging.info("%8s %s", "CTIME", ppls(absfile, filedata))
                change = True
            elif stats.st_uid != st_uid:
                logging.info("%8s %s", "UID", ppls(absfile, filedata))
                change = True
            elif stats.st_gid != st_gid:
                logging.info("%8s %s", "GID", ppls(absfile, filedata))
                change = True
            elif stats.st_mode != st_mode:
                logging.info("%8s %s", "MODE", ppls(absfile, filedata))
                change = True
            elif stats.st_size != st_size:
                logging.info("%8s %s", "SIZE", ppls(absfile, filedata))
                change = True
            # update data dictionary if something has changed
            if change is False:
                logging.debug("%8s %s", "OK", ppls(absfile, filedata))
            else:
                try:
                    with open(absfile, "rb") as infile:
                        metadata = filestorage.put(infile)
                        # update data
                        data["filedata"][absfile] = {
                            "checksum": metadata["checksum"],
                            "stat": (stats.st_mtime, stats.st_atime, stats.st_ctime, stats.st_uid, stats.st_gid, stats.st_mode, stats.st_size)
                        }
                        changed = True
                except PermissionError as exc:
                    logging.error(exc)
                    logging.error("skipping file %s", absfile)
    # search for new files on local storage
    for root, dirs, files in os.walk(data["path"]):
        for filename in files:
            absfilename = os.path.join(root, filename)
            if blacklist_func(absfilename):
                logging.debug("%8s %s", "EXCLUDE", absfilename)
                continue
            if os.path.isfile(absfilename) is False:
                logging.debug("%8s %s", "NOFILE", absfilename)
                continue
            if absfilename not in data["filedata"]:
                # there is some new file
                logging.info("%8s %s", "ADD", absfilename)
                try:
                    stats = os.stat(absfilename)
                    metadata = filestorage.put(open(absfilename, "rb"))
                    data["filedata"][absfilename] = {
                        "checksum": metadata["checksum"],
                        "stat": (stats.st_mtime, stats.st_atime, stats.st_ctime, stats.st_uid, stats.st_gid, stats.st_mode, stats.st_size)
                    }
                    changed = True
                except (OSError, IOError) as exc:
                    logging.error(exc)
    data["stoptime"] = time.time()
    data["totalcount"] = len(data["filedata"])
    data["totalsize"] = sum((data["filedata"][absfilename]["stat"][-1] for absfilename in data["filedata"].keys()))
    return changed


def test(filestorage, data, level=0):
    """
    check backup archive for consistency
    check if the filechecksum is available in FileStorage

    if deep is True also every block will be checked
        this operation could be very time consuming!
    """
    filecount = 0  # number of files
    fileset = set()  # unique list of filechecksums
    blockcount = 0  # number of blocks
    blockset = set()  # unique list of blockchecksums
    if level == 0:  # check only checksum existance in filestorage
        for absfile, filedata in data["filedata"].items():
            if filestorage.exist(filedata["checksum"]):
                logging.info("FILE-CHECKSUM %s EXISTS  for %s", filedata["checksum"], absfile)
                filecount += 1
                fileset.add(filedata["checksum"])
    elif level == 1:  # get filemetadata and check also block existance
        blockstorage = filestorage.blockstorage
        for absfile, filedata in data["filedata"].items():
            metadata = filestorage.get(filedata["checksum"])
            logging.info("FILE-CHECKSUM %s OK     for %s", filedata["checksum"], absfile)
            filecount += 1
            fileset.add(filedata["checksum"])
            for blockchecksum in metadata["blockchain"]:
                blockset.add(blockchecksum)
                if blockchecksum in blockstorage:
                    logging.info("BLOCKCHECKSUM %s EXISTS", blockchecksum)
                else:
                    logging.error("BLOCKCHECKSUM %s MISSING", blockchecksum)
                blockcount += 1
    elif level == 2:  # get filemetadata and read every block, very time consuming
        blockstorage = filestorage.blockstorage
        for absfile, filedata in data["filedata"].items():
            metadata = filestorage.get(filedata["checksum"])
            logging.info("FILE-CHECKSUM %s OK      for %s", filedata["checksum"], absfile)
            filecount += 1
            fileset.add(filedata["checksum"])
            for blockchecksum in metadata["blockchain"]:
                blockset.add(blockchecksum)
                blockstorage.get(blockchecksum)
                logging.info("BLOCKCHECKSUM %s OK", blockchecksum)
                blockcount += 1
    logging.info("all files %d(%d) available, %d(%d) blocks used", filecount, len(fileset), blockcount, len(blockset))


def restore(filestorage, data, targetpath, overwrite=False):
    """
    restore all files of archive to targetpath
    backuppath will be replaced by targetpath
    """
    # check if some files are missing or have changed
    for absfile in sorted(data["filedata"].keys()):
        filedata = data["filedata"][absfile]
        st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
        newfilename = absfile.replace(data["path"], targetpath)
        # remove double slashes
        if not os.path.isdir(os.path.dirname(newfilename)):
            logging.debug("creating directory %s", os.path.dirname(newfilename))
            os.makedirs(os.path.dirname(newfilename))
        if (os.path.isfile(newfilename)) and (overwrite is True):
            logging.info("REPLACE %s", newfilename)
            outfile = open(newfilename, "wb")
            for block in filestorage.read(filedata["checksum"]):
                outfile.write(block)
            outfile.close()
        elif (os.path.isfile(newfilename)) and (overwrite is False):
            logging.info("SKIPPING %s", newfilename)
        else:
            logging.info("RESTORE %s", newfilename)
            outfile = open(newfilename, "wb")
            for block in filestorage.read(filedata["checksum"]):
                outfile.write(block)
            outfile.close()
        try:
            os.chmod(newfilename, st_mode)
            os.utime(newfilename, (st_atime, st_mtime))
            os.chown(newfilename, st_uid, st_gid)
        except OSError as exc:
            logging.error(exc)


def restore_single(filestorage, filedata, targetpath, name, checksum, overwrite=False):
    """
    restore singl file, identified by name and checksum,
    to targetpath, named by basename in backupset
    """
    st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
    newfilename = os.path.join(targetpath, os.path.basename(name))
    # replace, skip or restore
    if (os.path.isfile(newfilename)) and (overwrite is True):
        logging.info("REPLACE %s", newfilename)
        outfile = open(newfilename, "wb")
        for block in filestorage.read(filedata["checksum"]):
            outfile.write(block)
        outfile.close()
    elif (os.path.isfile(newfilename)) and (overwrite is False):
        logging.info("SKIPPING %s", newfilename)
    else:
        logging.info("RESTORE %s", newfilename)
        outfile = open(newfilename, "wb")
        for block in filestorage.read(filedata["checksum"]):
            outfile.write(block)
        outfile.close()
    try:  # change permissions and times
        os.chmod(newfilename, st_mode)
        os.utime(newfilename, (st_atime, st_mtime))
        os.chown(newfilename, st_uid, st_gid)
    except OSError as exc:
        logging.error(exc)


def list_content(data: dict):
    """
    show archive content
    """
    # check if some files are missing or have changed
    filecount = 0
    sizecount = 0
    for absfile in sorted(data["filedata"].keys()):
        filedata = data["filedata"][absfile]
        logging.info(ppls(absfile, filedata))
        # st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
        filecount += 1
        sizecount += filedata["stat"][6]
    logging.info("%d files, total size %s", filecount, sizeof_fmt(sizecount))


def save_webstorage_archive(data: dict):
    """
    add duration, checksum and signature to data,
    afterwards store in WebStorageArchive
    """
    duration = data["stoptime"] - data["starttime"]
    logging.info("duration %0.2f s, bandwith %s /s", duration, sizeof_fmt(data["totalsize"] / duration))
    logging.info("%(totalcount)d files of %(totalsize)s bytes size", data)
    wsa = WebStorageArchiveClient()
    wsa.save(data)
    return


def get_webstorage_data(filename: str) -> dict:
    """
    return data from webstorage archive

    public_key ... path to public key file, to verify signature, if present
    filename ... to name a file, or otherwise use the latest available backupset
    """
    wsa = WebStorageArchiveClient()
    return wsa.read(filename)


def import_archive(filename: str, delete: bool = False, check: bool = True):
    """
    importing some archive in json.gz into central archive
    """
    wsa = WebStorageArchiveClient()
    exists = False
    if not os.path.isfile(filename):
        logging.error(f"file {filename} does not exist")
        sys.exit(1)
    with gzip.open(filename, "rt") as infile:
        data = json.loads(infile.read())
        key = wsa.get_key(data)
        if wsa.exists(key):
            logging.info(f"data already exist in archive {key}")
            logging.info(f"local checksum {data['checksum']}")
            logging.info(f"local number of files {len(data['filedata'])}")
            remote_data = wsa.read(key)
            logging.info(f"remote checksum {remote_data['checksum']}")
            logging.info(f"remote number of files {len(remote_data['filedata'])}")
            exists = True
        else:
            logging.info("archive does not exist, data will be analyzed")
            if check:
                for filename, filedata in data["filedata"].items():
                    logging.info("{filename} : {filestorage.exists(filedata['checksum'])}")
    if exists and delete:
        logging.info(f"deleting file {filename}")
        os.unlink(filename)


def export_archive(archive_name: str):
    """
    exporting archive from central storage to filesystem
    """
    wsa = WebStorageArchiveClient()
    data = wsa.read(archive_name)
    with gzip.open(archive_name, "wt") as outfile:
        outfile.write(json.dumps(data))


def main():
    """
    get options, then call specific functions
    """
    parser = argparse.ArgumentParser(description="create/manage/restore WebStorage Archives")
    parser.add_argument("name", nargs="*", help="filename or archive name or path")

    group_create = parser.add_argument_group("create backupset from scratch")
    group_create.add_argument("-c", "--create", action="store_true", help="create archive of this name")

    group_diff = parser.add_argument_group("create incremental backupset, some pre existing backupset must exist")
    group_diff.add_argument("-d", dest="diff", action="store_true", help="create differential to latest or given backupset name")

    group_extract = parser.add_argument_group("extract archive")
    group_extract.add_argument("-x", dest="extract", action="store_true", help="restore content of backupset to path location")
    # group_extract.add_argument("--backupset", help="backupset to get from backend, if not given use the latest available backupset")
    group_extract.add_argument("--overwrite", action="store_true", default=False, help="overwrite existing files during restore default %(default)s")
    # group_extract.add_argument("--extract-path", help="path to restore to")

    group_get = parser.add_argument_group("Extract single file from backupset")
    group_get.add_argument("-X", dest="extract_file", action="store_true", help="extract single files from backupset")
    # group_get.add_argument("--checksum", help="file checksum")
    # group_get.add_argument("--name", help="file checksum")

    group_test = parser.add_argument_group("testing of backupsets and retrieving existing archive informations")
    group_test.add_argument("-l", dest="list", action="store_true", help="list backupsets, use --backupset to specify one specific")
    group_test.add_argument("-L", dest="list_content", action="store_true", help="list content of backupset")
    group_test.add_argument("--list-checksums", action="store_true", default=False, help="in conjunction with -L to output also checksums")
    group_test.add_argument("-t", dest="test", action="store_true", help="verify archive, use --backupset to specify one specific")
    group_test.add_argument("--test-level", default=0, help="in conjunction with --test, 0=fast, 1=medium, 2=fully")

    group_optional = parser.add_argument_group("optional")
    group_optional.add_argument("--exclude-file", help="exclude file, in conjunction with --create and --diff")
    group_optional.add_argument("--tag", help="optional tag for this archive, otherwise last portion of path is used")
    group_optional.add_argument("--nocache", dest="cache", action="store_false", default=True, help="disable caching mode, using less memory")
    group_optional.add_argument("--hostname", dest="hostname", help="set specific hostname")

    group_special = parser.add_argument_group("some special functions")
    group_special.add_argument("--convert", action="store_true", help="convert wstar named ba sha256 checksum to newer key format")
    group_special.add_argument("--purge-cache", action="store_true", help="purge locally stored checksums database")
    group_special.add_argument("--import-archive", action="store_true", help="import archive from this local file")
    group_special.add_argument("--export-archive", action="store_true", help="export archive from storage to local directory")

    group_output = parser.add_mutually_exclusive_group()
    group_output.add_argument("-q", "--quiet", action="store_true", help="switch to loglevel ERROR")
    group_output.add_argument("-v", "--verbose", action="store_true", help="switch to loglevel DEBUG")

    args = parser.parse_args()
    # set logging level
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    # exclude file pattern of given
    blacklist_func = None
    if args.exclude_file is not None:
        logging.debug("using exclude file %s", args.exclude_file)
        blacklist_func = create_blacklist(args.exclude_file)
    else:
        blacklist_func = lambda a: False
    #
    #
    # MAIN OPTIONS Sections
    #
    if not args.hostname:
        args.hostname = socket.gethostname()
    wsa = WebStorageArchiveClient()
    filestorage = FileStorageClient(cache=args.cache)
    # CONVERT old style archive key naming on s3
    if args.convert:
        wsa.convert_keyname()
        wsa.convert_metadata()
    # EXPORT Archive
    elif args.export_archive:
        if not args.name[0]:
            logging.error("archive name missing")
            sys.exit(1)
        archive_name = args.name[0]
        export_archive(archive_name)
    # IMPORT Archive
    elif args.import_archive:
        if not args.name[0]:
            logging.error("local archive filename missing")
            sys.exit(1)
        import_filename = args.name[0]
        import_archive(import_filename)
    # PURGE Cache
    elif args.purge_cache:
        filestorage.purge_cache()  # will also purge BlockstorageCache
    # CREATE new Backupset
    elif args.create:
        if not args.name[0]:
            logging.error("local pathname missing")
            sys.exit(1)
        create_path = args.name[0]
        if not os.path.isdir(create_path):
            logging.error(f"{create_path} does not exist")
            sys.exit(1)
        if not args.tag:
            args.tag = os.path.basename(os.path.dirname(create_path))
        # create
        logging.info(f"archiving content of {create_path}")
        data = create(filestorage, create_path, blacklist_func, args.tag)
        save_webstorage_archive(data)
    # LIST Backupsets
    elif args.list:
        args.cache = False  # set this explicit, not useful
        # list all available backupsets
        if not args.backupset:  # list all available
            for value in wsa.get_backupsets(args.hostname):
                logging.info("%(date)10s %(time)8s %(hostname)s\t%(tag)s\t%(basename)s\t%(size)s", value)
        else:  # list content of specific backupset
            logging.info("getting backupset %s", args.backupset)
            data = get_webstorage_data(args.backupset)
            if data is not None:
                if args.list_checksums is True:
                    for absfile in sorted(data["filedata"].keys()):
                        filedata = data["filedata"][absfile]
                        logging.info("%s %s", filedata["checksum"], absfile)
                else:
                    list_content(data)
            else:
                logging.info("backupset not found found")
    # LIST Content of Archive
    elif args.list_content:
        if not args.name[0]:
            logging.error("archive name missing")
            sys.exit(1)
        archive_name = args.name[0]
        args.cache = False  # set this explicit, not useful
        logging.info(f"getting archive {archive_name}")
        data = get_webstorage_data(archive_name)
        if data is not None:
            if args.list_checksums is True:
                for absfile in sorted(data["filedata"].keys()):
                    filedata = data["filedata"][absfile]
                    logging.info(f"{filedata['checksum']} {absfile}")
            else:
                list_content(data)
        else:
            logging.info("archive not found")
    # TEST Backupset
    elif args.test:
        if not args.name:
            logging.info("using latest backupset, otherwise specify archive name")
            archive_name = wsa.get_latest_backupset(args.hostname)
        else:
            archive_name = args.name[0]
        logging.info(f"testing backupset {archive_name}")
        data = get_webstorage_data(archive_name)
        test(filestorage, data, level=int(args.test_level))
    # DIFFERENTIAL Backupset
    elif args.diff:
        if not args.name:
            logging.info("using latest backupset, otherwise specify archive name")
            archive_name = wsa.get_latest_backupset(args.hostname)
        else:
            archive_name = args.name[0]
        logging.info(f"creating differential backupset to existing backupset {archive_name}")
        data = get_webstorage_data(archive_name)
        changed = diff(filestorage, data, blacklist_func)
        if changed is False:
            logging.info("Nothing changed")
        else:
            save_webstorage_archive(data)
    # EXTRACT Backupset to path
    elif args.extract:
        # -x <archive_name> <destination_path>
        if len(args.name) != 2:
            logging.error("you have to provide first archive name and second destination path")
            sys.exit(1)
        archive_name = args.name[0]
        destination_path = args.name[1]
        if not os.path.isdir(destination_path):
            logging.error(f"folder {destination_path} to restore to does not exist")
            sys.exit(1)
        logging.info("restoring {archive_name} to {destination_path}")
        data = get_webstorage_data(archive_name)
        restore(filestorage, data, destination_path, overwrite=args.overwrite)
    # GET Backupset to path
    elif args.extract_file:
        # -X  <archive_name> <destination_path> <filename in archive>
        if len(args.name) != 3:
            logging.error("you have to provide first archive name and second destination path and third checksum to restore")
            sys.exit(1)
        archive_name = args.name[0]
        destination_path = args.name[1]
        filename = args.name[2]
        if not os.path.isdir(destination_path):
            logging.error(f"folder {destination_path} to restore file does not exist")
            sys.exit(1)
        data = get_webstorage_data(archive_name)
        if filename not in data["filedata"]:
            logging.error(f"provided filename {filename} does not exist in backupset")
            sys.exit(2)
        filedata = data["filedata"][filename]
        checksum = filedata["checksum"]
        restore_single(filestorage, filedata, destination_path, filename, checksum, overwrite=args.overwrite)
    else:
        logging.error("nice, you have started this program without any purpose?")


if __name__ == "__main__":
    main()
