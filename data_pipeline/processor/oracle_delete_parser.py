###############################################################################
# Module:    oracle_delete_parser
# Purpose:   Parses LogMiner Delete statements
#
# Notes:
#
###############################################################################


import data_pipeline.constants.const as const
from data_pipeline.sql.delete_statement import DeleteStatement
from .oracle_where_parser import OracleWhereParser


class OracleDeleteParser(OracleWhereParser):
    def __init__(self):
        super(OracleDeleteParser, self).__init__()

    def parse(self, table_name, commit_statement, primary_key_fields):
        pk_list = self._set_primary_keys_from_string(
            primary_key_fields,
            table_name)

        parsing_state = None
        seek_to_string = " {} ".format(const.WHERE)
        statement = DeleteStatement(table_name, primary_key_list=pk_list)
        self._init(commit_statement, parsing_state, seek_to_string, statement)

        super(OracleDeleteParser, self).parse()

        return self._statement
