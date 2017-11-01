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
# Module:    oracle_cdc_processor
# Purpose:   Processes Oracle CDCs retrieved from a stream
#
# Notes:
#
###############################################################################

import data_pipeline.constants.const as const
from data_pipeline.stream.oracle_message import OracleMessage
from .processor import Processor
from .oracle_insert_parser import OracleInsertParser
from .oracle_update_parser import OracleUpdateParser
from .oracle_delete_parser import OracleDeleteParser
from .oracle_alter_parser import OracleAlterParser
from .oracle_create_parser import OracleCreateParser


class OracleCdcProcessor(Processor):

    def __init__(self, metacols):
        super(OracleCdcProcessor, self).__init__()
        self._parsers = {
            const.ALTER_TABLE: OracleAlterParser(),
            const.CREATE_TABLE: OracleCreateParser(),
            const.INSERT: OracleInsertParser(metacols),
            const.UPDATE: OracleUpdateParser(metacols),
            const.DELETE: OracleDeleteParser(),
        }
        self._pcd = None

    def renew_workdirectory(self):
        super(OracleCdcProcessor, self).renew_workdirectory()
        for operation, parser in self._parsers.items():
            parser.renew_workdirectory()

    def deserialise(self, stream_msg):
        msg = OracleMessage()
        msg.deserialise(stream_msg)
        return msg

    def process(self, msg):
        """Parses the Oracle CDC statements
        :param OracleMessage msg: Oracle msg payload polled from queue
        :return: Statement object representing the statement to apply to target
        :rtype: Statement
        """

        parser = self._get_parser_for(msg)
        if not parser:
            self._logger.debug("Redo statement not supported. Skipping: {}"
                               .format(msg))
            return None
        elif msg.commit_statement.strip() == const.EMPTY_STRING:
            self._logger.warn("Redo statement is empty. Skipping")
            return None
        else:
            self._logger.debug("Processing msg...")

        statement = parser.parse(msg.table_name,
                                 msg.commit_statement,
                                 msg.primary_key_fields)

        self._logger.debug("Successfully processed msg...")
        return statement

    def _get_parser_for(self, msg):
        operation = msg.operation_code

        parser = None
        if operation == const.DDL:
            commit_statement = msg.commit_statement.upper()
            if const.ALTER_TABLE in commit_statement:
                parser = self._parsers[const.ALTER_TABLE]
            elif const.CREATE_TABLE in commit_statement:
                parser = self._parsers[const.CREATE_TABLE]
        else:
            parser = self._parsers.get(operation)

        if parser is None:
            self._logger.warn("Unsupported operation code: '{op}' for "
                              "statement: '{statement}'"
                              .format(op=msg.operation_code,
                                      statement=msg.commit_statement))
        return parser
