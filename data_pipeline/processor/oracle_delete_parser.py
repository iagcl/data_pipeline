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
