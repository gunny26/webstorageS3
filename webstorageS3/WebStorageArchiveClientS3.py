#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use FileStorage and BlockStorage WebApps
"""
import os
import sys
import re
import json
import logging
import base64
import gzip
import hashlib
from io import BytesIO
# non std modules
import yaml
import boto3
from botocore.exceptions import ClientError


class WebStorageArchiveClient():
    """
    store and retrieve Data, specific for WebStorageArchives
    """
    def __init__(self):
        """__init__"""
        self._logger = logging.getLogger(self.__class__.__name__)
        # according to platform search for config file in home directory
        if os.name == "nt":
            self._homepath = os.path.join(os.path.expanduser("~"), "AppData", "Local", "webstorage")
        else:
            self._homepath = os.path.join(os.path.expanduser("~"), ".webstorage")
        logging.debug("using config directory %s", self._homepath)
        if not os.path.isdir(self._homepath):
            print(f"first create directory {self._homepath} and place webstorage.yml file in there")
            sys.exit(1)
        configfile = os.path.join(self._homepath, "webstorage.yml")
        if os.path.isfile(configfile):
            with open(configfile, "rt") as infile:
                self._config = yaml.load(infile.read())
                # use proxy, if defined in config
                if "HTTP_PROXY" in self._config:
                    os.environ["HTTP_PROXY"] = self._config["HTTP_PROXY"]
                if "HTTPS_PROXY" in self._config:
                    os.environ["HTTPS_PROXY"] = self._config["HTTPS_PROXY"]
                self._client = boto3.client(
                    "s3",
                    aws_access_key_id=self._config["S3_ACCESS_KEY"],
                    aws_secret_access_key=self._config["S3_SECRET_KEY"],
                    endpoint_url=self._config["S3_ENDPOINT_URL"],
                    use_ssl=self._config["S3_USE_SSL"]
                    )
        else:
            print(f"configuration file {configfile} is missing")
            sys.exit(2)
        self._bucket_name = self._config["WEBSTORAGE_BUCKET_NAME"]
        self._logger.debug("bucket list: %s", self._client.list_buckets())

    def _encode(self, msg):
        """
        encode some string first to base64, next in hex notation
        """
        b64_msg = base64.b64encode(msg.encode("utf-8"))
        hex_msg = b64_msg.hex()
        return msg.encode("utf-8").hex()

    def _decode(self, hex_msg):
        """
        decode some hex notated string to base64 ascii, next to original message
        """
        b64_msg = bytes.fromhex(hex_msg)
        msg = base64.b64decode(r_filename64).decode("utf-8")
        return bytes.fromhex(hex_msg).decode("utf-8")

    def get_backupsets(self, hostname=None):
        """
        get all available backupsets
        works like directory listing of *.wstar.gz
        returns data sorted by datetime of filename

        :param hostname <str>: if not given use local hostname
        :return <list>: list of all stored backupsets for this hostname
        """
        objects = self._client.list_objects(Bucket=self._bucket_name)
        if "Contents" not in objects: # otherwise this bucket is empty
            return []
        result = {}
        rex = re.compile(r"^(.+)_(.+)_(.+)\.wstar\.gz$")
        for entry in objects["Contents"]:
            response = self._client.head_object(Bucket=self._bucket_name, Key=entry["Key"]) # TODO: exceptions
            size = response['ContentLength']
            if "Metadata" in response and response["Metadata"]:
                thishostname = response["Metadata"]["hostname"]
                tag = response["Metadata"]["tag"]
                timestamp = response["Metadata"]["datetime"]
                # 2016-10-25T20:23:17.782902
                thisdate, thistime = timestamp.split("T")
                thistime = thistime.split(".")[0]
                if hostname and hostname != thishostname: # filter only backupsets for this hostname
                    continue
                result[entry["Key"]] = {
                    "date": thisdate,
                    "time": thistime,
                    "datetime": timestamp,
                    "size": size,
                    "tag": tag,
                    "hostname": thishostname,
                    "basename": entry["Key"]
                }
        # sort by datetime
        return sorted(result.values(), key=lambda a: a["datetime"])

    def get_latest_backupset(self, hostname=None):
        """
        get the latest backupset stored shorthand function to get_backupsets

        :param hostname <str>: hsotname of client
        :returns <str>: filename of latest stored backupset for this hostname
        """
        try:
            return self.get_backupsets(hostname)[-1]["basename"]
        except IndexError:
            pass

    def read(self, filename):
        """
        read content of stored WebstorageArchive

        :param filename <str>: filename of archive, will base64 encoded
        :return <dict>: metadata of archive
        """
        b_buffer = BytesIO()
        self._client.download_fileobj(self._bucket_name, filename, b_buffer) # TODO: exceptions
        b_buffer.seek(0) # do not forget this tiny little line !!
        gzip_data = b_buffer.read()
        data = self._gunzip_bytes(gzip_data)
        return json.loads(data)

    def _gzip_str(self, data):
        """
        gzip some string and return bytes

        :param data <str>: some string, will be utf-8 encoded
        """
        out = BytesIO()
        with gzip.GzipFile(fileobj=out, mode='w') as outfile:
            outfile.write(data.encode("utf-8"))
        out.seek(0)
        return out

    def _gunzip_bytes(self, bytes_obj):
        """
        gunzip some bytes data to string

        :param bytes_obj <bytes>: as returned by _gzip_str
        :returns <str>:
        """
        in_ = BytesIO()
        in_.write(bytes_obj)
        in_.seek(0)
        with gzip.GzipFile(fileobj=in_, mode='rb') as fo:
            gunzipped_bytes_obj = fo.read()
        return gunzipped_bytes_obj.decode("utf-8")

    def save(self, data):
        """
        save data generated by wstar on webstorage

        :param data <dict>: information about stored files and directories
        """
       # build sha256 checksum
        sha256 = hashlib.sha256() # TODO: put this in config ?
        sha256.update(json.dumps(data, sort_keys=True).encode("utf-8"))
        data["checksum"] = sha256.hexdigest()
        self._logger.info("checksum of archive %s", data["checksum"])
        # store
        extra_args = {
            "Metadata": {
                "hostname" : data["hostname"],
                "tag": data["tag"],
                "datetime": data["datetime"]
            }
        }
        # f_object = BytesIO(json.dumps(data).encode("utf-8"))
        f_object = self._gzip_str(json.dumps(data))
        self._client.upload_fileobj(f_object, self._bucket_name, data["checksum"], ExtraArgs=extra_args)
