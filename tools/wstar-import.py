#!/usr/bin/python3
# pylint: disable=line-too-long
# disable=locally-disabled, multiple-statements, fixme, line-too-long
"""
command line program to create/restore/test WebStorageArchives
"""
import argparse
import datetime
import gzip
import hashlib
import json
import logging
import os
import re
import socket
import stat
import sys
import time
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')
# own modules
from webstorageS3 import WebStorageArchiveClient, FileStorageClient

def filemode(st_mode):
    """
    convert stat st_mode number to human readable string
    taken from https://stackoverflow.com/questions/17809386/how-to-convert-a-stat-output-to-a-unix-permissions-string
    """
    is_dir = 'd' if stat.S_ISDIR(st_mode) else '-'
    dic = {'7':'rwx', '6' :'rw-', '5' : 'r-x', '4':'r--', '0': '---'}
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
    return "%10s %s %s %10s %19s %s" % (filemode(st_mode), st_uid, st_gid, sizeof_fmt(st_size), datestring, absfile)

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
            operator = row.strip()[0] # -/+
            pattern = row.strip()[2:] # some string to use in match
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
        "PUT" : 0,
        "FDEDUP" : 0,
        "BDEDUP" : 0,
        "EXCLUDE" : 0,
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
                    "checksum" : metadata["checksum"],
                    "stat" : (stats.st_mtime, stats.st_atime, stats.st_ctime, stats.st_uid, stats.st_gid, stats.st_mode, stats.st_size)
                }
                if action_str == "PUT":
                    logging.error("%8s %s", action_str, ppls(absfilename, archive_dict["filedata"][absfilename]))
                else:
                    logging.info("%8s %s", action_str, ppls(absfilename, archive_dict["filedata"][absfilename]))
                action_stat[action_str] += 1
            except (OSError, IOError) as exc:
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
    data["starttime"] = time.time() # change to now
    data["datetime"] = datetime.datetime.today().isoformat() # change to now
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
            if  stats.st_mtime != st_mtime:
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
                            "checksum" : metadata["checksum"],
                            "stat" : (stats.st_mtime, stats.st_atime, stats.st_ctime, stats.st_uid, stats.st_gid, stats.st_mode, stats.st_size)
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
                        "checksum" : metadata["checksum"],
                        "stat" : (stats.st_mtime, stats.st_atime, stats.st_ctime, stats.st_uid, stats.st_gid, stats.st_mode, stats.st_size)
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
    filecount = 0 # number of files
    fileset = set() # unique list of filechecksums
    blockcount = 0 # number of blocks
    blockset = set() # unique list of blockchecksums
    if level == 0: # check only checksum existance in filestorage
        for absfile, filedata in data["filedata"].items():
            if filestorage.exist(filedata["checksum"]):
                logging.info("FILE-CHECKSUM %s EXISTS  for %s", filedata["checksum"], absfile)
                filecount += 1
                fileset.add(filedata["checksum"])
    elif level == 1: # get filemetadata and check also block existance
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
    elif level == 2: # get filemetadata and read every block, very time consuming
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
    try: # change permissions and times
        os.chmod(newfilename, st_mode)
        os.utime(newfilename, (st_atime, st_mtime))
        os.chown(newfilename, st_uid, st_gid)
    except OSError as exc:
        logging.error(exc)

def list_content(data):
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

def save_webstorage_archive(data):
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

def get_webstorage_data(filename=None):
    """
    return data from webstorage archive

    public_key ... path to public key file, to verify signature, if present
    filename ... to name a file, or otherwise use the latest available backupset
    """
    wsa = WebStorageArchiveClient()
    if filename is None:
        logging.info("-f not provided, using latest available archive")
        filename = wsa.get_latest_backupset(args.hostname)
    return wsa.read(filename)

def main():
    """
    get options, then call specific functions
    """
    parser = argparse.ArgumentParser(description="importing wstar data into webstoragearchive")
    parser.add_argument("path", help="file to read from, must be gzipped")
    parser.add_argument("--check", action="store_true", help="check if filechecksums are present")
    parser.add_argument("--delete", action="store_true", help="delete source file, if archive was uploaded or archive does already exist")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-q", "--quiet", action="store_true", help="switch to loglevel ERROR")
    group.add_argument("-v", "--verbose", action="store_true", help="switch to loglevel DEBUG")
    args = parser.parse_args()
    # set logging level
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    #
    #
    # MAIN OPTIONS Sections
    #
    tag = os.path.basename(os.path.dirname("~"))
    wsa = WebStorageArchiveClient()
    filestorage = FileStorageClient(cache=False)
    exists = False
    if not os.path.isfile(args.path):
        print(f"file {args.path} does not exist")
        sys.exit(1)
    with gzip.open(args.path, "rt") as infile:
        data = json.loads(infile.read())
        key = wsa.get_key(data)
        if wsa.exists(key):
            print(f"data already exist in archive {key}")
            print(f"local checksum {data['checksum']}")
            print(f"local number of files {len(data['filedata'])}")
            remote_data = wsa.read(key)
            print(f"remote checksum {remote_data['checksum']}")
            print(f"remote number of files {len(remote_data['filedata'])}")
            exists = True
        else:
            print("archive does not exist, data will be analyzed")
            if args.check:
                for filename, filedata in data["filedata"].items():
                    print("{filename} : {filestorage.exists(filedata['checksum'])}")
    if exists and args.delete:
        print(f"deleting file {args.path}")
        os.unlink(args.path)

if __name__ == "__main__":
    main()
