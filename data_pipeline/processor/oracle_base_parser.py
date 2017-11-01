# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# 
###############################################################################
# Module:    oracle_base_parser
# Purpose:   Base class for oracle parser
#
# Notes:
#
###############################################################################

import logging
import data_pipeline.constants.const as const

from abc import ABCMeta, abstractmethod
from .exceptions import UnsupportedSqlError


class OracleBaseParser(object):

    def __init__(self):
        self._commit_statement = None
        self._set_logger()
        self._parsing_state = None
        self._char_buff = None
        self._read_cursor = None

    def _set_logger(self):
        self._logger = logging.getLogger(__name__)

    def renew_workdirectory(self):
        self._set_logger()

    @abstractmethod
    def parse(self, table_name, commit_statement, primary_key_fields=None):
        pass

    @abstractmethod
    def _set_special_value(self, special_value_rule):
        pass

    @property
    def _curr_char(self):
        return self._commit_statement[self._read_cursor]

    @property
    def _next_char(self):
        if self._end_of_line():
            return None
        return self._commit_statement[self._read_cursor + 1]

    def _end_of_line(self):
        return self._read_cursor == len(self._commit_statement) - 1

    def set_commit_statement(self, value):
        self._commit_statement = value

    def set_read_cursor(self, value):
        self._read_cursor = value

    def _init(self, commit_statement=const.EMPTY_STRING, parsing_state=None,
              seek_to_string=const.EMPTY_STRING, statement=None):
        self._char_buff = const.EMPTY_STRING
        self._commit_statement = commit_statement
        self._read_cursor = self._get_index_after_string(seek_to_string)
        self._parsing_state = parsing_state
        self._statement = statement

    def _get_index_after_string(self, string):
        return (self._commit_statement.upper().index(string.upper()) +
                len(string))

    def _consume(self):
        self._char_buff += self._curr_char

    def _flush_buffer(self):
        buff_contents = self._char_buff
        self._empty_buffer()
        return buff_contents

    def _empty_buffer(self):
        self._char_buff = const.EMPTY_STRING

    def _next(self):
        self._read_cursor += 1

    def _set_data_type(self, data_type):
        data_type = data_type.upper().strip()

        if data_type not in const.ORACLE_SUPPORTED_DATATYPES:
            err_message = ("The '{data_type}' datatype is not supported. "
                           "Statement ignored: {statement}"
                           .format(data_type=data_type,
                                   statement=self._commit_statement))
            raise UnsupportedSqlError(err_message)

        self._active_entry[const.DATA_TYPE] = data_type

    def _set_constraint(self, constraint):
        constraint = constraint.strip()
        if constraint:
            self._active_entry[const.CONSTRAINTS] = constraint

    def _set_params(self, params):
        self._active_entry[const.PARAMS] = params

    def _add_param(self, param):
        params = self._active_entry[const.PARAMS]

        # Character data types should only have 1 parameter
        if self._is_char_data_type() and len(params) > 0:
            return

        params.append(param.strip())

    def _is_char_data_type(self):
        if const.DATA_TYPE in self._active_entry:
            return 'CHAR' in self._active_entry[const.DATA_TYPE]
        return False

    def _set_field_name(self, field_name):
        self._active_entry[const.FIELD_NAME] = field_name.strip()

    def _set_operation(self, operation):
        operation = operation.upper().strip()
        if operation not in const.SUPPORTED_OPERATIONS:
            err_message = ("Alter operation '{operation}' is not supported. "
                           "Skipping...".format(operation=operation))
            raise UnsupportedSqlError(err_message)

        self._active_entry[const.OPERATION] = operation

    def _process_special_values(self):
        for special_string in self._special_value_rules:

            if self._is_special_string_at_cursor(special_string):
                rule = self._special_value_rules[special_string]
                self._empty_buffer()
                self._set_special_value(rule)
                self._parsing_state = rule[const.NEW_PARSING_STATE]
                self._read_cursor += len(special_string)
                return True

        return False

    def _is_special_string_at_cursor(self, special_string):
        start = self._read_cursor
        end = start + len(special_string)
        return self._commit_statement[start:end] == special_string

    def _at_escaped_single_quote(self):
        return (self._curr_char == const.SINGLE_QUOTE and
                self._next_char == const.SINGLE_QUOTE)
