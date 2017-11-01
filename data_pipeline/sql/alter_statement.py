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
# Module:    alter_statement
# Purpose:   Represents SQL alter statements
#
# Notes:
#
###############################################################################

import data_pipeline.sql.utils as sql_utils
import data_pipeline.constants.const as const

from .ddl_statement import DdlStatement


class AlterStatement(DdlStatement):
    """Contains data necessary to produce a valid SQL ALTER statement"""

    def __init__(self, table_name):
        super(AlterStatement, self).__init__(table_name)
        self.statement_type = const.ALTER

    def add_entry(self, **kwargs):
        if const.ALTER_ENTRY in kwargs:
            self.entries.append(kwargs[const.ALTER_ENTRY])
        else:
            alter_entry = {
                const.OPERATION: kwargs[const.OPERATION],
                const.FIELD_NAME: kwargs[const.FIELD_NAME],
                const.DATA_TYPE: kwargs[const.DATA_TYPE],
                const.PARAMS: kwargs[const.PARAMS],
                const.CONSTRAINTS: kwargs[const.CONSTRAINTS]
            }
            self.add_entry(alter_entry=alter_entry)

    def tosql(self, applier):
        return applier.build_alter_sql(self)

    def __str__(self):
        return sql_utils.build_alter_sql(self)
