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
# Module:    where_statement
# Purpose:   Represents a SQL where statement
#
# Notes:
#
###############################################################################

import data_pipeline.sql.utils as sql_utils
import data_pipeline.constants.const as const

from .base_statement import BaseStatement


class WhereStatement(BaseStatement):
    """Contains the necessary data necessary for producing a valid SQL WHERE
    clause. Assumption at the moment is that each condition is a conjuction of
    ANDs, hence no conjunction data is stored in this class
    """
    def __init__(self, table_name, conditions, primary_key_list):
        super(WhereStatement, self).__init__(table_name)
        if conditions:
            self.conditions = conditions
        else:
            self.conditions = {}

        if primary_key_list is None:
            self.primary_key_list = []
        else:
            self.primary_key_list = primary_key_list

    def add_condition(self, field_name, value):
        self.conditions[field_name.upper()] = value

    def tosql(self, applier):
        pass

    def __str__(self):
        return sql_utils.build_where_sql(self, const.SPECIAL_CHAR_REPLACEMENT)
