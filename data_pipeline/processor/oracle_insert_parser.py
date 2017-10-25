###############################################################################
# Module:    oracle_insert_parser
# Purpose:   Parses LogMiner Insert statements
#
# Notes:
#
###############################################################################

import csv
import re
from enum import Enum

import data_pipeline.constants.const as const
from .oracle_base_parser import OracleBaseParser
from data_pipeline.sql.insert_statement import InsertStatement

InsertState = Enum('InsertState',
                   'fields fieldskip interfieldvalue values valueskip')


class OracleInsertParser(OracleBaseParser):
    def __init__(self, metacols):
        super(OracleInsertParser, self).__init__()
        self._metacols = metacols
        self._in_function = False
        self._consuming_in_function = False
        self._field_values = None
        self._fields = None
        self._values = None
        self._special_value_rules = {
            const.NULL: {
                const.ASSIGN_VALUE: None,
                const.NEW_PARSING_STATE: InsertState.valueskip
            },
            const.ORACLE_EMPTY_CLOB: {
                const.ASSIGN_VALUE: None,
                const.NEW_PARSING_STATE: InsertState.valueskip
            },
            const.ORACLE_EMPTY_BLOB: {
                const.ASSIGN_VALUE: None,
                const.NEW_PARSING_STATE: InsertState.valueskip
            }
        }

    def parse(self, table_name, commit_statement, primary_key_fields):
        self._field_values = {}
        self._fields = []
        self._values = []

        self._init(commit_statement, InsertState.fieldskip, const.LEFT_BRACKET,
                   InsertStatement(table_name, self._field_values))

        while self._read_cursor < len(self._commit_statement):
            if self._parsing_field_or_value():
                self._consume()

            self._transition_insert_parsing_state()
            self._next()

        self._add_metacols()

        for (field, value) in zip(self._fields, self._values):
            self._field_values[field] = value

        return self._statement

    def _add_metacols(self):
        if self._metacols:
            self._add_metacol(const.METADATA_INSERT_TS_COL)
            self._add_metacol(const.METADATA_UPDATE_TS_COL)

    def _add_metacol(self, metacolname):
        if metacolname in self._metacols:
            metafield = self._metacols[metacolname].upper()
            self._fields.append(metafield)
            self._values.append(const.METADATA_CURRENT_TIME_SQL)

    def _set_special_value(self, special_value_rule):
        self._values.append(special_value_rule[const.ASSIGN_VALUE])

    def _parsing_field_or_value(self):
        return (self._parsing_state == InsertState.fields or
                self._parsing_state == InsertState.values)

    def _transition_insert_parsing_state(self):
        if self._parsing_state == InsertState.fields:
            self._transition_from_fields_parsing()

        elif self._parsing_state == InsertState.fieldskip:
            self._transition_from_fieldskip_parsing()

        elif self._parsing_state == InsertState.interfieldvalue:
            self._transition_from_interfieldvalue_parsing()

        elif self._parsing_state == InsertState.values:
            self._transition_from_values_parsing()

        elif self._parsing_state == InsertState.valueskip:
            self._transition_from_valueskip_parsing()

        else:
            raise Exception("Unsupported state: {}"
                            .format(self._parsing_state))

    def _transition_from_fields_parsing(self):
        if self._at_field_word_boundary():
            self._fields.append(self._flush_buffer()[:-1])
            self._parsing_state = InsertState.fieldskip

    def _transition_from_fieldskip_parsing(self):
        if self._at_field_word_boundary():
            self._empty_buffer()
            self._parsing_state = InsertState.fields

        elif self._at_field_group_end():
            self._parsing_state = InsertState.interfieldvalue

    def _transition_from_interfieldvalue_parsing(self):
        if self._at_value_group_start():
            self._parsing_state = InsertState.valueskip

    def _transition_from_values_parsing(self):
        if self._at_escaped_single_quote():
            self._next()

        elif self._at_value_word_boundary():
            self._values.append(self._flush_buffer()[:-1])
            self._parsing_state = InsertState.valueskip

            # First value parsed in function, ignore the rest. e.g.:
            # TO_DATE('2017-06-06 15:36:24', 'YYYY-MM-DD HH24:MI:SS')
            if self._in_function:
                self._consuming_in_function = False

    def _transition_from_valueskip_parsing(self):
        if self._process_special_values():
            pass

        elif self._at_function_group_start():
            self._in_function = True
            self._consuming_in_function = True

        elif self._at_function_group_end():
            self._in_function = False

        elif self._at_value_word_boundary():
            if self._can_start_consuming_value():
                self._empty_buffer()
                self._parsing_state = InsertState.values

    def _can_start_consuming_value(self):
        return (not self._in_function or
                (self._in_function and self._consuming_in_function))

    def _at_field_word_boundary(self):
        return self._curr_char == const.DOUBLE_QUOTE

    def _at_field_group_end(self):
        return self._curr_char == const.RIGHT_BRACKET

    def _at_value_group_start(self):
        return self._curr_char == const.LEFT_BRACKET

    def _at_value_word_boundary(self):
        return self._curr_char == const.SINGLE_QUOTE

    def _at_value_group_end(self):
        return self._curr_char == const.RIGHT_BRACKET

    def _at_function_group_start(self):
        return self._curr_char == const.LEFT_BRACKET

    def _at_function_group_end(self):
        return self._curr_char == const.RIGHT_BRACKET
