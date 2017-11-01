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
# Module:    delete_statement
# Purpose:   Represents a SQL delete statement
#
# Notes:
#
###############################################################################

import data_pipeline.sql.utils as sql_utils
import data_pipeline.constants.const as const

from .where_statement import WhereStatement


class DeleteStatement(WhereStatement):
    """Contains data necessary for producing a valid SQL DELETE statement"""

    def __init__(self, table_name, where_condition_key_values=None,
                 primary_key_list=None):
        """Construct a new DeleteStatement instance

        The constructor parameters relate to the SQL DELETE statement like so:

        DELETE FROM <table_name>
        WHERE <for each where_condition_key_values>

        :param str table_name: The table name for this statement
        :param dict where_condition_key_values:
            The dictionary of key-value pairs of conditions in the WHERE clause
        :param list primary_key_list: The list of primary keys
        """
        super(DeleteStatement, self).__init__(table_name,
                                              where_condition_key_values,
                                              primary_key_list)
        self.statement_type = const.DELETE

    def tosql(self, applier):
        return applier.build_delete_sql(self)

    def __str__(self):
        return sql_utils.build_delete_sql(self)
