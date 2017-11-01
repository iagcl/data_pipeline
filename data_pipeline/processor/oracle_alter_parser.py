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
# Module:    oracle_alter_parser
# Purpose:   Parses LogMiner Alter statements
#
# Notes:
#
###############################################################################


import data_pipeline.constants.const as const
from enum import Enum
from data_pipeline.sql.alter_statement import AlterStatement
from .oracle_base_parser import OracleBaseParser

AlterState = Enum('AlterState',
                  'operation field_name data_type params constraints')


class OracleAlterParser(OracleBaseParser):

    def __init__(self):
        super(OracleAlterParser, self).__init__()
        self._stack = []
        self._consuming = False

    def _init_active_entry(self):
        op = self._active_entry[const.OPERATION] if self._stack else None
        self._active_entry = {
            const.OPERATION: op,
            const.FIELD_NAME: None,
            const.DATA_TYPE: None,
            const.PARAMS: [],
            const.CONSTRAINTS: const.EMPTY_STRING,
        }

    def parse(self, table_name, commit_statement, primary_key_fields=None):
        self._init(commit_statement, AlterState.operation, table_name,
                   AlterStatement(table_name))
        self._init_active_entry()

        while True:
            if self._consuming:
                self._consume()
            self._next()
            if self._read_cursor >= len(self._commit_statement):
                break

            self._transition_alter_parsing_state()

        return self._statement

    def _transition_alter_parsing_state(self):

        if self._parsing_state == AlterState.operation:
            self._transition_from_operation_parsing()

        elif self._parsing_state == AlterState.field_name:
            self._transition_from_field_name_parsing()

        elif self._parsing_state == AlterState.data_type:
            self._transition_from_data_type_parsing()

        elif self._parsing_state == AlterState.params:
            self._transition_from_params_parsing()

        elif self._parsing_state == AlterState.constraints:
            self._transition_from_constraints_parsing()

    def _transition_from_operation_parsing(self):
        if self._at_word_boundary():
            if self._consuming:
                self._set_operation(self._flush_buffer())
                self._parsing_state = AlterState.field_name
                self._consuming = False

        elif not self._consuming:
            self._consuming = True

    def _transition_from_field_name_parsing(self):
        if self._at_word_boundary():

            if self._curr_char == const.LEFT_BRACKET:
                self._stack.append(self._curr_char)

            if self._curr_char == const.RIGHT_BRACKET:
                self._stack.pop()
                self._parsing_state = AlterState.operation

            elif self._consuming:
                self._set_field_name(self._flush_buffer().strip())
                self._parsing_state = AlterState.data_type

            self._consuming = False

        elif not self._consuming:
            self._consuming = True

    def _transition_from_data_type_parsing(self):
        if self._end_of_line() and self._curr_char != const.RIGHT_BRACKET:
            self._consume()

        if self._at_word_boundary():
            self._set_data_type(self._flush_buffer())

            end_of_entry_reached = self._end_of_line()

            # Data type has parameters
            if self._curr_char == const.LEFT_BRACKET:
                self._parsing_state = AlterState.params
                self._stack.append(self._curr_char)

            # Data type has no parameters and end of operation group
            else:
                if self._curr_char == const.RIGHT_BRACKET:
                    self._stack.pop()

                # A comma seen while parsing the data type indicates
                # the end of the entry
                if self._curr_char == const.COMMA:
                    end_of_entry_reached = True

                if end_of_entry_reached:
                    self._add_entry()

                    # If we're within an operation group
                    if self._stack:
                        self._parsing_state = AlterState.field_name
                    # Otherwise, look for the next operation
                    else:
                        self._parsing_state = AlterState.operation
                # We haven't reached the end of the entry yet,
                # so look for constraints
                else:
                    self._parsing_state = AlterState.constraints

            self._consuming = False

        elif not self._consuming:
            self._consuming = True

    def _transition_from_params_parsing(self):
        if self._at_word_boundary():
            param = self._flush_buffer().strip()
            if param:
                self._add_param(param)

            # We've reached the end of the parameters
            if self._curr_char == const.RIGHT_BRACKET:
                self._stack.pop()
                self._parsing_state = AlterState.constraints

            if self._end_of_line():
                self._add_entry()

            # If we see a word boundary, such as a space,
            # ignore everything afterwards. e.g. VARCHAR2(256 CHAR)
            self._consuming = False

        elif not self._consuming:
            self._consuming = True

    def _transition_from_constraints_parsing(self):
        if self._at_word_boundary():
            end_of_entry_reached = self._end_of_line()

            if self._curr_char == const.RIGHT_BRACKET:
                end_of_entry_reached = True
                self._stack.pop()

            # We won't support space-separated entries, even though
            # Oracle allows for it
            if self._curr_char == const.COMMA:
                end_of_entry_reached = True

            if end_of_entry_reached:
                constraint = self._flush_buffer().strip()
                self._set_constraint(constraint)

                self._add_entry()

                if self._stack:
                    self._parsing_state = AlterState.field_name
                else:
                    self._parsing_state = AlterState.operation

                self._consuming = False

        elif not self._consuming:
            self._consuming = True

    def _add_entry(self):
        self._statement.add_entry(alter_entry=self._active_entry)
        self._init_active_entry()

    def _at_word_boundary(self):
        return (self._end_of_line() or
                self._curr_char == const.LEFT_BRACKET or
                self._curr_char == const.RIGHT_BRACKET or
                self._curr_char == const.COMMA or
                self._curr_char == const.SPACE)
