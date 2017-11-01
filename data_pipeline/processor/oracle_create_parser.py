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
# Module:    oracle_create_parser
# Purpose:   Parses LogMiner Create statements
#
# Notes:
#
###############################################################################


import re
import data_pipeline.constants.const as const

from enum import Enum
from data_pipeline.sql.create_statement import CreateStatement
from .oracle_base_parser import OracleBaseParser


CreateState = Enum('CreateState', 'field_name data_type params constraints')
constraint_pattern = re.compile(",\s*(CONSTRAINT\s)")


def _get_create_body(create_statement):
    first_left_bracket_i = create_statement.index(const.LEFT_BRACKET)
    last_right_bracket_i = create_statement.rindex(const.RIGHT_BRACKET)
    return create_statement[first_left_bracket_i + 1:last_right_bracket_i]


def _separate_constraints(create_body):
    fields = constraint_pattern.split(create_body)
    constraints = []
    if len(fields) > 1:
        constraints_fields = fields[1:]
        i = 0
        while i < len(constraints_fields):
            concat_constr = constraints_fields[i] + constraints_fields[i + 1]
            constraints.append(concat_constr)
            i += 2

    fields = fields[0]
    return (fields, constraints)


def _end_of_line(i, string):
    return i >= len(string) - 1


class OracleCreateParser(OracleBaseParser):

    def __init__(self):
        super(OracleCreateParser, self).__init__()
        self._stack = []
        self._consuming = False

    def _init_active_entry(self):
        self._active_entry = {
            const.FIELD_NAME: None,
            const.DATA_TYPE: None,
            const.PARAMS: [],
            const.CONSTRAINTS: None
        }

    def parse(self, table_name, commit_statement, primary_key_fields=None):
        create_body = _get_create_body(commit_statement)
        (create_fields, constraints) = _separate_constraints(create_body)
        self._init(statement=CreateStatement(table_name))
        self._init_active_entry()
        self._update_all_indices(create_fields)

        self._logger.debug("Parsing={}".format(create_fields))
        while True:
            # At the last entry, which doesn't contain commas
            # e.g. ..., id varchar2(10))
            if self._at_last_entry(create_fields):
                entry = create_fields[self._read_cursor:]
                self._parse_entry(entry)
                break

            # At a middle entry, which doesn't contain commas
            # e.g. ..., id varchar2(10), ...
            elif self._at_param_entry(create_fields):
                entry = create_fields[self._read_cursor:self.next_comma_index]
                self._read_cursor = self.next_comma_index + 1
                self._parse_entry(entry)
                self._update_all_indices(create_fields)

            # At a middle entry, which doesn't contain commas or brackets.
            # e.g. ..., id varchar2, ...
            elif self._at_entry(create_fields):
                entry = create_fields[self._read_cursor:self.next_comma_index]
                self._read_cursor = self.next_comma_index + 1
                self._parse_entry(entry)
                self._update_all_indices(create_fields)

            # At a middle entry that contains commas.
            # e.g.: ..., id number(10,20), ...
            elif self._at_param_entry_with_commas(create_fields):
                self.next_comma_index = create_fields.find(
                    const.COMMA, self.next_comma_index + 1)

        # Then add any individual constraints
        for constraint in constraints:
            self._parse_entry(constraint)

        return self._statement

    def _at_entry(self, create_fields):
        return (self.next_comma_index < self.next_left_bracket_index and
                self.next_comma_index < self.next_right_bracket_index)

    def _at_param_entry(self, create_fields):
        """At a middle parameterised data_type entry, without commas.
        e.g. ..., id varchar2(10), ...
        """
        return (self.next_comma_index > self.next_right_bracket_index and
                self.next_comma_index > self.next_left_bracket_index)

    def _at_param_entry_with_commas(self, create_fields):
        """At a middle parameterised entry with commas.
        e.g.: ..., id number(10,20), ...
        """
        return (self.next_comma_index > self.next_left_bracket_index and
                self.next_comma_index < self.next_right_bracket_index)

    def _at_last_entry(self, create_fields):
        """At the last entry, without commas  e.g. ..., id varchar2(10))"""
        return self.next_comma_index == -1

    def _update_all_indices(self, create_fields):
        self._set_next_comma_index(create_fields)
        self._set_next_left_bracket_index(create_fields)
        self._set_next_right_bracket_index(create_fields)

    def _set_next_comma_index(self, create_fields):
        self.next_comma_index = create_fields.find(const.COMMA,
                                                   self._read_cursor)

    def _set_next_left_bracket_index(self, create_fields):
        self.next_left_bracket_index = create_fields.find(
            const.LEFT_BRACKET, self._read_cursor)

    def _set_next_right_bracket_index(self, create_fields):
        self.next_right_bracket_index = create_fields.find(
            const.RIGHT_BRACKET, self._read_cursor)

    def _parse_entry(self, entry_str):
        self._logger.debug("Parsing entry={}".format(entry_str))
        entry_str = entry_str.strip()
        entry_parts = entry_str.split(' ', 1)
        field_name = entry_parts[0].strip()

        if field_name == const.CONSTRAINT:
            self._set_constraint(entry_str)
        else:
            self._set_field_name(field_name)
            rest = entry_parts[1]

            first_left_bracket_i = rest.find(const.LEFT_BRACKET)
            first_right_bracket_i = rest.find(const.RIGHT_BRACKET)
            space_i = rest.find(const.SPACE)
            has_params = first_right_bracket_i > -1
            has_constraints = space_i > -1

            constraint = None

            if has_params:
                self._set_data_type(rest[:first_left_bracket_i])
                paramstr = rest[first_left_bracket_i + 1:first_right_bracket_i]
                paramlist = paramstr.split(const.COMMA)
                self._set_params(map(lambda x: x.strip(), paramlist))
                self._set_constraint(rest[first_right_bracket_i + 1:])

            elif has_constraints:
                self._set_data_type(rest[:space_i])
                self._set_constraint(rest[space_i + 1:])

            else:
                self._set_data_type(rest)

        self._add_active_entry()

    def _add_active_entry(self):
        self._statement.add_entry(create_entry=self._active_entry)
        self._logger.debug("Adding create_entry={}".format(self._statement))
        self._init_active_entry()
