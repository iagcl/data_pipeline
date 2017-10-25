###############################################################################
# Module:    oracledb
# Purpose:   Wraps the native Oracle client, exposing required operations on
#            Oracle databases
#
# Notes:
#
###############################################################################

import cx_Oracle
import logging
import data_pipeline.constants.const as const

from .db import Db
from .oracle_query_results import OracleQueryResults


def makedsn(conn_details):
    dsn_str = None
    if conn_details.host and conn_details.port and conn_details.dbsid:
        dsn_str = cx_Oracle.makedsn(
            conn_details.host, conn_details.port, conn_details.dbsid)
    elif conn_details.host:
        dsn_str = conn_details.host

    return dsn_str


class OracleDb(Db):
    def __init__(self):
        super(OracleDb, self).__init__()

    @property
    def dbtype(self):
        return const.ORACLE

    def _connect(self, connection_details):
        self._connection_details = connection_details
        try:
            dsn_str = makedsn(connection_details)
            self._logger.info(
                "Connecting to Oracle: [dbuserid={}, dsn={}]"
                .format(connection_details.userid,
                        dsn_str))

            userid_parts = connection_details.userid.split(" as ")
            # Hard-coded check for sysdba usernames in
            # order to set the correct connect mode
            if len(userid_parts) > 1 and userid_parts[1].lower() == "sysdba":
                self._connection = cx_Oracle.connect(
                    user=userid_parts[0], mode=cx_Oracle.SYSDBA,
                    password=connection_details.password, dsn=dsn_str)
            else:
                self._connection = cx_Oracle.connect(
                    user=userid_parts[0],
                    password=connection_details.password,
                    dsn=dsn_str)

            if not self._cursor:
                self._cursor = self._connection.cursor()

            self._logger.info("Connected to {}".format(dsn_str))
        except cx_Oracle.DatabaseError, exception:
            self._logger.error(
                "Failed to connect to '{}': {}"
                .format(connection_details.host, exception.message))
            exit(1)

    def execute_stored_proc(self, stored_proc):
        decorated_stored_proc = "begin {}; end;".format(stored_proc)
        self._logger.debug(
            "Executing stored proc '{}'"
            .format(decorated_stored_proc))
        self._cursor.execute(decorated_stored_proc)

    def execute_query(self, query, arraysize, values=(),
                      post_process_func=None):
        self._logger.debug(
            "Executing query '{q}'. Arraysize={s}"
            .format(q=query, s=arraysize))
        self._cursor.arraysize = arraysize
        self._cursor.execute(query)
        query_results = OracleQueryResults(self._cursor)
        self._logger.debug("Query execution complete")
        return query_results

    def is_client_present(self):
        pass
