###############################################################################
# Module:    query_results
# Purpose:   An interface definition for retrieving query results
#
# Notes:
###############################################################################

from abc import ABCMeta, abstractmethod


class QueryResults(object):
    ___metaclass__ = ABCMeta

    def __init__(self):
        pass

    def __iter__(self):
        return self

    @abstractmethod
    def next(self):
        pass

    @abstractmethod
    def fetchone(self):
        pass

    @abstractmethod
    def fetchall(self):
        pass

    @abstractmethod
    def fetchmany(self, arraysize=None):
        pass
