###############################################################################
# Module:    oracle_query_results
# Purpose:   Represents the query result object returned from a Oracle query
#            execution.
#
# Notes:
###############################################################################

import data_pipeline.constants.const as const

from .db_query_results import DbQueryResults


class OracleQueryResults(DbQueryResults):
    def __init__(self, cursor):
        super(OracleQueryResults, self).__init__(cursor)

    def _is_complete(self, record):
        return self.get(record, const.CSF_FLAG_FIELD) == '0'

    def _append_redo_statement(self, parent_record, child_record):
        partial_redo = self.get(child_record, const.SQLREDOSTMT_FIELD)
        redo_col_i = self.get_col_index(const.SQLREDOSTMT_FIELD)
        parent_record[redo_col_i] += partial_redo

    def next(self):
        parent_record = super(OracleQueryResults, self).next()
        parent_record = list(parent_record)

        if const.CSF_FLAG_FIELD not in self.get_col_names():
            return parent_record

        parent_statement_id = self.get(parent_record, const.STATEMENT_ID_FIELD)
        curr_record = parent_record

        while not self._is_complete(curr_record):
            curr_record = super(OracleQueryResults, self).next()
            curr_statement_id = self.get(curr_record, const.STATEMENT_ID_FIELD)

            if curr_statement_id != parent_statement_id:
                raise ValueError(
                    "Invalid state: Multiline flag is set "
                    "in previous message but statement_ids differ: {} != {}"
                    .format(curr_statement_id, parent_statement_id))

            self._append_redo_statement(parent_record, curr_record)

        return parent_record
