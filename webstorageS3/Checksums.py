#!/usr/bin/python3
import logging
import os
import sqlite3


class Checksums:
    """
    storing persistent set in sqlite database
    """

    def __init__(self, filename: str):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._checksums = set()
        self._filename = filename
        self._logger.info("using filename %s", filename)
        if not os.path.isfile(filename):
            self._logger.info("creating empty database")
            self._con = sqlite3.connect(filename)
            self._cur = self._con.cursor()
            sqlstring = """
            CREATE TABLE IF NOT EXISTS
            tbl_checksums(checksum char(40) UNIQUE)
            """
            self._cur.execute(sqlstring)
        else:
            self._logger.info("using existing database")
            self._con = sqlite3.connect(filename)
            self._cur = self._con.cursor()
            sqlstring = """
            SELECT checksum FROM tbl_checksums
            """
            result = self._cur.execute(sqlstring)
            for entry in result:
                self._checksums.add(entry[0])
            self._logger.info(f"loaded {len(self._checksums)} existing checksums")

    def update(self, checksums: str):
        """
        import some list of checksums to database and memory
        :param checksums <list>:
        """
        for checksum in checksums:
            self._cur.execute(f"INSERT INTO tbl_checksums VALUES('{checksum}')")
            self._checksums.add(checksum)
        self._con.commit()

    def add(self, checksum: str):
        """
        add single checksum to set and database
        :param checksum <str>: checksum to store
        """
        assert len(checksum) == 40
        if checksum not in self._checksums:  # otherwise unique constraint error
            try:
                self._cur.execute(f"INSERT INTO tbl_checksums VALUES('{checksum}')")
                self._con.commit()
                self._checksums.add(checksum)
            except sqlite3.IntegrityError as exc:
                self._logger.exception(exc)
                self._logger.error(f"error adding checksum {checksum} to cache")

    def __contains__(self, checksum: str) -> bool:
        if len(checksum) != 40:
            return False
        return checksum in self._checksums

    def __len__(self) -> int:
        return len(self._checksums)

    def __iter__(self) -> set:
        return self._checksums.__iter__()
