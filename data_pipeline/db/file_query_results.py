###############################################################################
# Module:    file_query_results
# Purpose:   Represents the query result object returned from a file query
#
# Notes:
###############################################################################

import itertools

from .query_results import QueryResults
from data_pipeline.stream.file_reader import FileReader


def default_post_process_func(line):
    return line


class FileQueryResults(QueryResults):

    def __init__(self, filename, post_process_func):
        super(FileQueryResults, self).__init__()
        self._handle = FileReader(filename)

        if post_process_func is None:
            self._post_process_func = default_post_process_func
        else:
            self._post_process_func = post_process_func

    def __iter__(self):
        return self

    def next(self):
        line = self._handle.readline().strip('\n')
        if not line:
            self._handle.close()
            raise StopIteration
        return self._post_process_func(line)

    def fetchone(self):
        line = None
        try:
            line = self.next()
        except StopIteration, e:
            pass
        return line

    def fetchall(self):
        return [self._post_process_func(l.strip('\n'))
                for l in self._handle]

    def fetchmany(self, arraysize=None):
        if arraysize > 0:
            return [self._post_process_func(l.strip('\n'))
                    for l in itertools.islice(self._handle, arraysize)]
        return self.fetchall()

    def __del__(self):
        self._handle.close()
