###############################################################################
# Module:    update_statement
# Purpose:   Represents a SQL update statement
#
# Notes:
#
###############################################################################

import data_pipeline.constants.const as const
import data_pipeline.sql.utils as sql_utils
from .where_statement import WhereStatement


class UpdateStatement(WhereStatement):
    """Contains data necessary for producing a valid SQL UPDATE statement"""

    def __init__(self, table_name, set_values=None,
                 where_condition_key_values=None, primary_key_list=None):
        """Construct a new UpdateStatement instance

        The constructor parameters relate to the SQL UPDATE statement like so:

        UPDATE <table_name>
        SET <set_values>
        WHERE <for each where_condition_key_values>

        :param str table_name: The table name for this statement
        :param dict set_values: The dictionary of key-value pairs
            of values in the SET clause
        :param dict where_condition_key_values:
            The dictionary of key-value pairs of conditions in the WHERE clause
        :param list primary_key_list: The list of primary keys
        """
        super(UpdateStatement, self).__init__(table_name,
                                              where_condition_key_values,
                                              primary_key_list)
        self.set_values = set_values if set_values else {}
        self.statement_type = const.UPDATE

    def tosql(self, applier):
        return applier.build_update_sql(self)

    def add_set_value(self, field_name, value):
        self.set_values[field_name] = value

    def __str__(self):
        return sql_utils.build_update_sql(self)
