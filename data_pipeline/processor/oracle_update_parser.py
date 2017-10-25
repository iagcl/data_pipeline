###############################################################################
# Module:    oracle_update_parser
# Purpose:   Parses LogMiner Update statements
#
# Notes:
#
###############################################################################


import data_pipeline.constants.const as const
from enum import Enum
from data_pipeline.sql.update_statement import UpdateStatement
from .oracle_where_parser import OracleWhereParser


UpdateState = Enum('UpdateState',
                   'set_start set_key set_operator set_value where_start')


class OracleUpdateParser(OracleWhereParser):

    def __init__(self, metacols):
        super(OracleUpdateParser, self).__init__()
        self._metacols = metacols
        self._special_value_rules = {
            const.ORACLE_SET_NULL: {
                const.ASSIGN_VALUE: None,
                const.NEW_PARSING_STATE: UpdateState.set_start
            },
            const.ORACLE_EMPTY_CLOB: {
                const.ASSIGN_VALUE: None,
                const.NEW_PARSING_STATE: UpdateState.set_start
            }
        }

    def parse(self, table_name, commit_statement, primary_key_fields):
        pk_list = self._set_primary_keys_from_string(
            primary_key_fields,
            table_name)

        parsing_state = UpdateState.set_start
        seek_to_string = " {} ".format(const.SET)
        statement = UpdateStatement(table_name, primary_key_list=pk_list)
        self._init(commit_statement, parsing_state, seek_to_string, statement)

        if self._metacols and const.METADATA_UPDATE_TS_COL in self._metacols:
            metafield = self._metacols[const.METADATA_UPDATE_TS_COL].upper()
            statement.add_set_value(metafield, const.METADATA_CURRENT_TIME_SQL)

        while self._read_cursor < len(self._commit_statement):
            if self._parsing_set():
                self._consume()
            elif self._parsing_state == UpdateState.where_start:
                break

            self._transition_update_parsing_state()
            self._next()

        super(OracleUpdateParser, self).parse()

        return self._statement

    def _set_special_value(self, special_value_rule):
        self._statement.add_set_value(self._curr_key,
                                      special_value_rule[const.ASSIGN_VALUE])

    def _parsing_set(self):
        return (self._parsing_state == UpdateState.set_start or
                self._parsing_state == UpdateState.set_key or
                self._parsing_state == UpdateState.set_value)

    def _transition_update_parsing_state(self):
        if self._parsing_state == UpdateState.set_start:
            self._transition_from_set_start_parsing()

        elif self._parsing_state == UpdateState.set_key:
            self._transition_from_set_key_parsing()

        elif self._parsing_state == UpdateState.set_operator:
            self._transition_from_set_operator_parsing()

        elif self._parsing_state == UpdateState.set_value:
            self._transition_from_set_value_parsing()

        else:
            raise Exception("Unsupported state: {}"
                            .format(self._parsing_state))

    def _transition_from_set_start_parsing(self):
        if self._at_key_word_boundary():
            self._empty_buffer()
            self._parsing_state = UpdateState.set_key

        elif const.WHERE in self._char_buff:
            self._empty_buffer()
            self._parsing_state = UpdateState.where_start

    def _transition_from_set_key_parsing(self):
        if self._at_key_word_boundary():
            self._curr_key = self._flush_buffer()
            self._next()
            self._parsing_state = UpdateState.set_operator

    def _transition_from_set_operator_parsing(self):
        if self._process_special_values():
            pass
        elif self._at_value_word_boundary():
            self._empty_buffer()
            self._parsing_state = UpdateState.set_value

    def _transition_from_set_value_parsing(self):
        if self._at_escaped_single_quote():
            self._next()

        elif self._at_value_word_boundary():
            set_value = self._flush_buffer()
            self._statement.add_set_value(self._curr_key, set_value)
            self._parsing_state = UpdateState.set_start
