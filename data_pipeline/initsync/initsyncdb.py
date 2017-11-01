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
# Module:    initsync_db
# Purpose:   Contains database endpoint specific functions for initsync
#
# Notes:     This module will route calls to the module responsible for the
#            concrete implementation of functionality.
#
###############################################################################

from abc import ABCMeta, abstractmethod


class InitSyncDb(object):
    __metaclass__ = ABCMeta

    def __init__(self, argv, db, logger):
        self._argv = argv
        self._db = db
        self._logger = logger

    @property
    def encoding(self):
        return self._db.encoding

    @abstractmethod
    def get_source_column_list(self, table):
        pass

    @abstractmethod
    def get_target_column_list(self, table):
        pass

    @abstractmethod
    def table_exists(self, table):
        pass

    @abstractmethod
    def bulk_write(self, **kwargs):
        pass

    def connect(self, connection_details):
        self._db.connect(connection_details)

    def commit(self):
        self._db.commit()

    @abstractmethod
    def delete(self, table, query_condition):
        pass

    @abstractmethod
    def truncate(self, table):
        pass

    def extract_data(self, column_list, table, query_condition, log_function):
        self._pre_extract()

        extract_data_sql = self._build_extract_data_sql(
            column_list, table, self._argv.extractlsn, self._argv.samplerows,
            self._argv.lock, query_condition)

        log_function(self._argv, table, extract_data_sql)

        return self._db.execute_query(extract_data_sql,
                                      self._argv.arraysize,
                                      post_process_func=self._post_extract)

    @abstractmethod
    def _pre_extract(self):
        """This will be run prior to the extract_data sql query, useful for
        setting up session-specific configuration.
        """
        pass

    @abstractmethod
    def _post_extract(self, record):
        """This will be run on a per-record basis to modify
        data at runtime as necessary"""
        pass

    @abstractmethod
    def _build_extract_data_sql(self, column_list, table, samplerows, lock):
        pass

    @abstractmethod
    def execute_post_processing(self, target_table):
        pass

    def close(self):
        self._db.close()
