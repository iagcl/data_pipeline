###############################################################################
# Module:    create_statement
# Purpose:   Represents SQL create statements
#
# Notes:
#
###############################################################################

import data_pipeline.sql.utils as sql_utils
import data_pipeline.constants.const as const

from .ddl_statement import DdlStatement


class CreateStatement(DdlStatement):
    """Contains data necessary to produce a valid SQL CREATE TABLE statement"""

    def __init__(self, table_name):
        super(CreateStatement, self).__init__(table_name)
        self.statement_type = const.CREATE

    def add_entry(self, **kwargs):
        if const.CREATE_ENTRY in kwargs:
            self.entries.append(kwargs[const.CREATE_ENTRY])
        else:
            create_entry = {
                const.FIELD_NAME: kwargs[const.FIELD_NAME],
                const.DATA_TYPE: kwargs[const.DATA_TYPE],
                const.PARAMS: kwargs[const.PARAMS],
                const.CONSTRAINTS: kwargs[const.CONSTRAINTS]
            }
            self.add_entry(create_entry=create_entry)

    def tosql(self, applier):
        return applier.build_create_sql(self)

    def __str__(self):
        return sql_utils.build_create_sql(self)
