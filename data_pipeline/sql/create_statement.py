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
# Module:    create_statement
# Purpose:   Represents SQL create statements
#
# Notes:
#
###############################################################################

import data_pipeline.sql.utils as sql_utils
import data_pipeline.constants.const as const

from .ddl_statement import DdlStatement


class CreateStatement(DdlStatement):
    """Contains data necessary to produce a valid SQL CREATE TABLE statement"""

    def __init__(self, table_name):
        super(CreateStatement, self).__init__(table_name)
        self.statement_type = const.CREATE

    def add_entry(self, **kwargs):
        if const.CREATE_ENTRY in kwargs:
            self.entries.append(kwargs[const.CREATE_ENTRY])
        else:
            create_entry = {
                const.FIELD_NAME: kwargs[const.FIELD_NAME],
                const.DATA_TYPE: kwargs[const.DATA_TYPE],
                const.PARAMS: kwargs[const.PARAMS],
                const.CONSTRAINTS: kwargs[const.CONSTRAINTS]
            }
            self.add_entry(create_entry=create_entry)

    def tosql(self, applier):
        return applier.build_create_sql(self)

    def __str__(self):
        return sql_utils.build_create_sql(self)
