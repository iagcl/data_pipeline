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
# Module:    insert_statement
# Purpose:   Represents SQL insert statements
#
# Notes:
#
###############################################################################

import data_pipeline.constants.const as const
from data_pipeline.sql.utils import build_value_sql

from .base_statement import BaseStatement


class InsertStatement(BaseStatement):
    """Contains data necessary for producing a valid SQL INSERT statement"""

    def __init__(self, table_name, field_values):
        super(InsertStatement, self).__init__(table_name)
        self._field_values = field_values
        self.statement_type = const.INSERT

    def get_value(self, field_name):
        return self._field_values.get(field_name)

    def get_fields(self):
        return self._field_values.keys()

    def contains_field(self, field_name):
        return field_name in self._field_values

    def tosql(self, applier):
        return applier.build_insert_sql(self)

    def __str__(self):
        field_names = list(self._field_values.keys())
        field_names.sort()

        field_values = [build_value_sql(self._field_values[f])
                        for f in field_names]

        sqlstr = ("INSERT INTO {table_name} ( {field_names} ) "
                  "VALUES ( {field_values} )"
                  .format(table_name=self.table_name,
                          field_names=const.COMMASPACE.join(field_names),
                          field_values=const.COMMASPACE.join(field_values)))
        return sqlstr
