###############################################################################
# Module:    ddl_statement
# Purpose:   Parent class for DDL (Data Definition Language) statements
#
# Notes:
#
###############################################################################

import data_pipeline.constants.const as const

from abc import ABCMeta, abstractmethod
from .base_statement import BaseStatement


class DdlStatement(BaseStatement):
    """Contains data necessary for producing a valid DDL statement"""

    __metaclass__ = ABCMeta

    def __init__(self, table_name):
        super(DdlStatement, self).__init__(table_name)
        self._entries = []

    @property
    def entries(self):
        return self._entries

    @abstractmethod
    def add_entry(self, **kwargs):
        pass

    def _build_field_params(self, params):
        if params:
            return "({})".format(const.COMMASPACE.join(params))
        return const.EMPTY_STRING

    def _build_field_string(self, value):
        return " {}".format(value if value else const.EMPTY_STRING)
