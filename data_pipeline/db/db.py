###############################################################################
# Module:    db
# Purpose:   Abstract class for interacting with a database
#
# Notes:
#
###############################################################################

import logging

from abc import ABCMeta, abstractmethod, abstractproperty


class Db(object):
    __metaclass__ = ABCMeta

    @abstractproperty
    def dbtype(self):
        pass

    @property
    def encoding(self):
        return self._connection.encoding

    @property
    def cursor(self):
        return self._cursor

    def __init__(self):
        self._set_logger()
        self._connection = None
        self._connected = False
        self._cursor = None
        self._connection_details = None

    def _set_logger(self):
        self._logger = logging.getLogger(__name__)

    def renew_workdirectory(self):
        self._set_logger()

    def connect(self, connection_details):
        if not connection_details:
            return

        self._connect(connection_details)
        self._connected = True

    @abstractmethod
    def _connect(self, connection_details):
        pass

    @abstractmethod
    def execute_stored_proc(self, stored_proc):
        pass

    @abstractmethod
    def execute_query(self, query, arraysize, values=(),
                      post_process_func=None):
        pass

    def execute(self, sql, values=(), log_sql=True):
        """Executes the sql string
        :param sql: The sql string to execute
        :param values: The values to bind to the sql string
        :return: The number of rows affected by the execute
        """
        # Only log this in verbose mode because it is used by our custom_orm
        # module which has a large number of parameters, resulting in very
        # large SQL statements being logged.
        if log_sql:
            self._logger.debug("Executing {sql}\nBind values = {v}"
                               .format(sql=sql, v=values))
        try:
            self._cursor.execute(sql, values)
            return self._cursor.rowcount
        except Exception, e:
            self._logger.exception("Failed to execute sql. {}".format(str(e)))
            raise

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    def close(self):
        self.disconnect()

    def closed(self):
        return not self._connected

    def disconnect(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

        self._logger.info("Closed {dbtype} connection"
                          .format(dbtype=self.dbtype))

        self._connected = False

    def __del__(self):
        self.disconnect()
