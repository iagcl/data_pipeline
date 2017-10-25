###############################################################################
# Module:    oracle_where_parser
# Purpose:   Parses Where component of LogMiner SQL statements
#
# Notes:
#
###############################################################################


from enum import Enum
import data_pipeline.constants.const as const
from .oracle_base_parser import OracleBaseParser

WhereState = Enum('WhereState', 'start key operator value')


class OracleWhereParser(OracleBaseParser):
    def __init__(self):
        super(OracleWhereParser, self).__init__()
        self._primarykey_list = None
        self._curr_key = None
        self._statement = None

    def _set_primary_keys_from_string(self, pkstr, table_name):
        pkstr = pkstr.strip()
        if pkstr:
            if pkstr == const.NO_KEYFIELD_STR:
                self._primarykey_list = []
            else:
                self._primarykey_list = [pk.strip().upper()
                                         for pk in pkstr.split(const.COMMA)]

        self._logger.debug(
            "[{table}] Set primary keys = {pks}"
            .format(table=table_name, pks=self._primarykey_list))

        return self._primarykey_list

    def set_primary_keys(self, value):
        self._primarykey_list = value

    def set_statement(self, value):
        self._statement = value

    def parse(self):
        self._char_buff = const.EMPTY_STRING
        self._parsing_state = WhereState.start

        while self._read_cursor < len(self._commit_statement):
            if self._can_consume_char():
                self._consume()

            self._transition_where_parsing_state()
            self._next()

    def _can_consume_char(self):
        return (self._parsing_state == WhereState.key or
                self._parsing_state == WhereState.value)

    def _transition_where_parsing_state(self):
        if self._parsing_state == WhereState.start:
            self._transition_from_where_start_parsing()

        elif self._parsing_state == WhereState.key:
            self._transition_from_where_key_parsing()

        elif self._parsing_state == WhereState.operator:
            self._transition_from_where_operator_parsing()

        elif self._parsing_state == WhereState.value:
            self._transition_from_where_value_parsing()

    def _transition_from_where_start_parsing(self):
        if self._at_key_word_boundary():
            self._empty_buffer()
            self._parsing_state = WhereState.key

    def _transition_from_where_key_parsing(self):
        if self._at_key_word_boundary():
            self._curr_key = self._flush_buffer()
            self._parsing_state = WhereState.operator

    def _transition_from_where_operator_parsing(self):
        is_null_index = self._commit_statement.find(const.IS_NULL,
                                                    self._read_cursor)

        if self._read_cursor == is_null_index:
            # Prefer using None over const.IS_NULL in case a
            # key's value is actually 'IS NULL'
            self._statement.add_condition(self._curr_key, None)

            self._read_cursor += len(const.IS_NULL)
            self._empty_buffer()
            self._parsing_state = WhereState.start

        elif self._at_value_word_boundary():
            self._empty_buffer()
            self._parsing_state = WhereState.value

    def _transition_from_where_value_parsing(self):
        if self._at_escaped_single_quote():
            self._next()

        elif self._at_value_word_boundary():
            where_value = self._flush_buffer()
            self._statement.add_condition(self._curr_key, where_value)
            self._parsing_state = WhereState.start

    def _can_add_primary_key(self):
        return (self._primarykey_list is None or
                const.NO_KEYFIELD_STR in self._primarykey_list or
                self._curr_key.upper() in self._primarykey_list)

    def _at_key_word_boundary(self):
        return self._curr_char == const.DOUBLE_QUOTE

    def _at_value_word_boundary(self):
        return self._curr_char == const.SINGLE_QUOTE

    def _flush_buffer(self):
        # Always remove last char which is only useful when
        # there are escaped single-quotes
        return super(OracleWhereParser, self)._flush_buffer()[:-1]
