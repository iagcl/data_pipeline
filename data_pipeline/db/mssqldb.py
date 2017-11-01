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
# Module:    mssqldb
# Purpose:   Wraps the native MSSQL client, exposing required operations on
#            MSSQL databases
#
# Notes:
#
###############################################################################

import pymssql
import logging
import data_pipeline.constants.const as const

from .db import Db
from .db_query_results import DbQueryResults
from .exceptions import MssqlDatabaseException


class MssqlDb(Db):
    def __init__(self):
        super(MssqlDb, self).__init__()

    @property
    def dbtype(self):
        return const.MSSQL

    def _connect(self, connection_details):
        self._connection_details = connection_details
        try:
            self._logger.info("Connecting to MSSQL Server - userid:'{userid}' "
                              "server:'{server}' port:{port} database:'{db}'"
                              .format(userid=connection_details.userid,
                                      server=connection_details.host,
                                      port=connection_details.port,
                                      db=connection_details.dbsid))
            self._connection = pymssql.connect(
                host=connection_details.host,
                port=connection_details.port,
                user=connection_details.userid,
                password=connection_details.password,
                database=connection_details.dbsid,
                charset=connection_details.charset,
                as_dict=False)

            self._cursor = self._connection.cursor()
            self._logger.info("Connected to '{}'"
                              .format(connection_details.dbsid))
        except MssqlDatabaseException , exception:
            self._logger.error("Failed to connect to '{}': {}"
                               .format(connection_details.dbsid,
                                       exception.message))
            exit(1)

    def execute_stored_proc(self, stored_procedure):
        self._logger.debug("Executing stored proc '{}'"
                           .format(stored_procedure))
        self._cursor.callproc(stored_procedure)

    def execute_query(self, query, arraysize=None, values=(),
                      post_process_func=None):
        self._logger.debug("Executing query '{}'".format(query))
        self._cursor.execute(query)
        query_results = DbQueryResults(self._cursor)
        self._logger.debug("Query execution complete")
        return query_results

    def is_client_present(self):
        pass
