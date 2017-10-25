###############################################################################
# Module:    db_query_results
# Purpose:   Represents the query result object returned from a DB API query
#            execution.
#
# Notes:
###############################################################################

from .query_results import QueryResults

COL_NAME_INDEX = 0


class DbQueryResults(QueryResults):
    def __init__(self, cursor):
        super(DbQueryResults, self).__init__()
        self._cursor = cursor
        self._col_map = {}
        self._col_names = []
        self._build_col_map()

    def __iter__(self):
        return self

    def __str__(self):
        return str(self._col_map)

    def next(self):
        record = self._cursor.fetchone()
        if not record:
            raise StopIteration
        else:
            return record

    def fetchone(self):
        record = None
        try:
            record = self.next()
        except StopIteration, e:
            pass
        return record

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchmany(self, arraysize=None):
        if arraysize is None:
            return self._cursor.fetchmany()
        return self._cursor.fetchmany(arraysize)

    def get_col_index(self, col):
        return self._col_map[col]

    def get_col_names(self):
        return self._col_names

    def _build_col_map(self):
        i = 0
        for description in self._cursor.description:
            self._col_map[description[COL_NAME_INDEX]] = i
            self._col_names.append(description[COL_NAME_INDEX])
            i += 1

    def get(self, record, column_name):
        return record[self.get_col_index(column_name)]
