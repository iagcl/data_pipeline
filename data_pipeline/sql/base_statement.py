###############################################################################
# Module:    base_statement
# Purpose:   A base class for representing SQL statements
#
# Notes:
#
###############################################################################

from abc import ABCMeta, abstractmethod


class BaseStatement(object):
    __metaclass__ = ABCMeta

    def __init__(self, table_name):
        self.table_name = table_name
        self.statement_type = None

    @abstractmethod
    def tosql(self, applier):
        pass
