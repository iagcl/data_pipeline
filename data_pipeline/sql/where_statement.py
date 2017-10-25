###############################################################################
# Module:    where_statement
# Purpose:   Represents a SQL where statement
#
# Notes:
#
###############################################################################

import data_pipeline.sql.utils as sql_utils
import data_pipeline.constants.const as const

from .base_statement import BaseStatement


class WhereStatement(BaseStatement):
    """Contains the necessary data necessary for producing a valid SQL WHERE
    clause. Assumption at the moment is that each condition is a conjuction of
    ANDs, hence no conjunction data is stored in this class
    """
    def __init__(self, table_name, conditions, primary_key_list):
        super(WhereStatement, self).__init__(table_name)
        if conditions:
            self.conditions = conditions
        else:
            self.conditions = {}

        if primary_key_list is None:
            self.primary_key_list = []
        else:
            self.primary_key_list = primary_key_list

    def add_condition(self, field_name, value):
        self.conditions[field_name.upper()] = value

    def tosql(self, applier):
        pass

    def __str__(self):
        return sql_utils.build_where_sql(self, const.SPECIAL_CHAR_REPLACEMENT)
