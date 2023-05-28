#!/usr/bin/python3
import logging
import os
import sqlite3

logger = logging.getLogger(__name__)


class Checksums:
    """
    storing persistent set in sqlite database
    """

    def __init__(self, filename: str) -> None:
        self._filename = filename

        self._checksums = set()  # uniqueset of checksums
        self._con = None  # database connection
        self._cur = None  # database cursor

        if not os.path.isfile(filename):
            self._create_database(filename)
        else:
            self._load_checksums(filename)

    def __contains__(self, checksum: str) -> bool:
        if len(checksum) != 40:  # sha1 checksums ar always 40 characters long
            return False
        return checksum in self._checksums

    def __iter__(self) -> set:
        return self._checksums.__iter__()

    def __len__(self) -> int:
        return len(self._checksums)

    def _create_database(self, filename: str) -> None:
        logger.debug(f"creating empty database {filename}")
        self._con = sqlite3.connect(filename)
        self._cur = self._con.cursor()
        sqlstring = """
        CREATE TABLE IF NOT EXISTS
        tbl_checksums(checksum char(40) UNIQUE)
        """
        self._cur.execute(sqlstring)

    def _load_checksums(self, filename: str) -> None:
        logger.debug(f"using existing database {filename}")
        self._con = sqlite3.connect(filename)
        self._cur = self._con.cursor()
        sqlstring = """
        SELECT checksum FROM tbl_checksums
        """
        result = self._cur.execute(sqlstring)
        for entry in result:
            self._checksums.add(entry[0])
        logger.info(f"loaded {len(self._checksums)} checksums from cache")

    def update(self, checksums: str) -> None:
        """
        import some list of checksums to database and memory
        :param checksums <list>:
        """
        for checksum in checksums:
            if len(checksum) != 40:
                raise AttributeError("sha1 checksums are always 40 characters long")
            self._cur.execute(f"INSERT INTO tbl_checksums VALUES('{checksum}')")
            self._checksums.add(checksum)
        self._con.commit()

    def add(self, checksum: str) -> None:
        """
        add single checksum to set and database
        :param checksum <str>: checksum to store
        """
        if len(checksum) != 40:
            raise AttributeError("sha1 checksums are always 40 characters long")
        if checksum not in self._checksums:  # otherwise unique constraint error
            try:
                self._cur.execute(f"INSERT INTO tbl_checksums VALUES('{checksum}')")
                self._con.commit()
                self._checksums.add(checksum)
            except sqlite3.IntegrityError as exc:
                logger.exception(exc)
                logger.error(f"error adding checksum {checksum} to cache")
