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
# Module:    db_query_results
# Purpose:   Represents the query result object returned from a DB API query
#            execution.
#
# Notes:
###############################################################################

from .query_results import QueryResults

COL_NAME_INDEX = 0


class DbQueryResults(QueryResults):
    def __init__(self, cursor):
        super(DbQueryResults, self).__init__()
        self._cursor = cursor
        self._col_map = {}
        self._col_names = []
        self._build_col_map()

    def __iter__(self):
        return self

    def __str__(self):
        return str(self._col_map)

    def next(self):
        record = self._cursor.fetchone()
        if not record:
            raise StopIteration
        else:
            return record

    def fetchone(self):
        record = None
        try:
            record = self.next()
        except StopIteration, e:
            pass
        return record

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchmany(self, arraysize=None):
        if arraysize is None:
            return self._cursor.fetchmany()
        return self._cursor.fetchmany(arraysize)

    def get_col_index(self, col):
        return self._col_map[col]

    def get_col_names(self):
        return self._col_names

    def _build_col_map(self):
        i = 0
        for description in self._cursor.description:
            self._col_map[description[COL_NAME_INDEX]] = i
            self._col_names.append(description[COL_NAME_INDEX])
            i += 1

    def get(self, record, column_name):
        return record[self.get_col_index(column_name)]
