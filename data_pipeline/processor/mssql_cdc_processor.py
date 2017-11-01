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
# Module:    mssql_cdc_processor
# Purpose:   Processes MSSQL CDCs polled from Kafka queue
#
# Notes:
#
###############################################################################

import csv
import re
import json
import logging

import data_pipeline.constants.const as const
import data_pipeline.sql.utils as utils
from data_pipeline.stream.mssql_message import MssqlMessage
from .processor import Processor

from data_pipeline.sql.insert_statement import InsertStatement
from data_pipeline.sql.update_statement import UpdateStatement
from data_pipeline.sql.delete_statement import DeleteStatement

INSERT_VALUES_DIALECT = 'insert_values'
INSERT_FIELDS_DIALECT = 'insert_fields'
START_POSITION = 0

csv.register_dialect(INSERT_FIELDS_DIALECT,
                     quotechar=const.DOUBLE_QUOTE)

csv.register_dialect(INSERT_VALUES_DIALECT,
                     doublequote=True,
                     quotechar=const.SINGLE_QUOTE)

_rm_function_re = re.compile(r",[A-Za-z0-9_]+\(('[^\)]+')\)")


def _rm_functions(s):
    return _rm_function_re.sub(r",\1", s)


def _parse_insert(table_name, column_names, column_values):
    field_values = dict()
    field_values = utils.build_field_value_list(column_names, column_values,
                                                START_POSITION)

    return InsertStatement(table_name, field_values)


def _parse_update(table_name, column_names, column_values, key_field_string):
    field_values = dict()
    set_values = utils.build_field_value_list(column_names, column_values,
                                              START_POSITION)

    key_field_list = utils.build_key_field_list(key_field_string)
    where_values = {k: set_values[k] for k in key_field_list}

    return UpdateStatement(table_name, set_values, where_values)


def _parse_delete(table_name, column_names, column_values, key_field_list):
    field_values = dict()
    field_values = utils.build_field_value_list(column_names, column_values,
                                                START_POSITION)

    return DeleteStatement(table_name, field_values)


class MssqlCdcProcessor(Processor):
    def __init__(self):
        self._logger = logging.getLogger(__name__)

    def deserialise(self, stream_message):
        msg = MssqlMessage()
        msg.deserialise(stream_message)
        return msg

    def process(self, msg):
        """Parses the MSSQL CDC statements
        :param MssqlMessage msg: MSSQL message payload polled from queue
        :return: Statement object representing the statement to apply to target
        :rtype: Statement
        """

        operation = msg.operation_code

        if operation == const.MSSQL_INSERT_ACTION:
            self._logger.debug("Processing INSERT: {}".format(msg))
            return _parse_insert(msg.table_name, msg.column_names,
                                 msg.column_values)

        elif operation == const.MSSQL_UPDATE_ACTION:
            self._logger.debug("Processing UPDATE: {}".format(msg))
            return _parse_update(msg.table_name, msg.column_names,
                                 msg.column_values, msg.primary_key_fields)

        elif operation == const.MSSQL_DELETE_ACTION:
            self._logger.debug("Processing DELETE: {}".format(msg))
            return _parse_delete(msg.table_name, msg.column_names,
                                 msg.column_values, msg.primary_key_fields)

        raise ValueError("Unsupported operation code: '{}'"
                         .format(msg.operation_code))
