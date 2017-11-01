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
# Module:    filedb
# Purpose:   Represents a file-based "database"
#
# Notes:
#
###############################################################################

import glob
import os
import data_pipeline.constants.const as const
import data_pipeline.utils.filesystem as fsutils

from .file_query_results import FileQueryResults
from .db import Db

from os.path import basename
from data_pipeline.stream.file_reader import FileReader


class FileDb(Db):
    def __init__(self):
        super(FileDb, self).__init__()
        self._file_reader = None
        self._closed = True  # Maintains the "closed" state for posterity
        self._data_dir = None

    @property
    def dbtype(self):
        return const.FILE

    def _connect(self, connection_details):
        self._closed = False
        self._data_dir = connection_details.data_dir

    def execute_stored_proc(self, stored_proc):
        pass

    def execute_query(self, tablename, arraysize, values=(),
                      post_process_func=None):
        """Return wrapper around file handle that reads line by line
        :param str tablename
            Files containing this tablename as its basename
            will be read line-by-line as the query result.
        :param int arraysize Unused
        :param tuple values Unused
        :param tuple post_process_func
            Function to execute on each record after retrieval
        """
        if tablename is None:
            return

        filename = self.get_data_filename(tablename)
        if filename is None:
            return

        self._logger.info("Querying file: {f}".format(f=filename))
        return FileQueryResults(filename, post_process_func)

    def get_data_filename(self, tablename):
        glob_pattern = "{}*".format(os.path.join(self._data_dir, tablename))
        matching_data_files = fsutils.insensitive_glob(glob_pattern)

        for f in matching_data_files:
            filename_with_ext = basename(f)
            dot_i = filename_with_ext.find(const.DOT, 1)
            if dot_i > 0:
                filename_without_ext = filename_with_ext[:dot_i]
            else:
                filename_without_ext = filename_with_ext

            if filename_without_ext.lower() == tablename.lower():
                return f
        return None

    def execute(self, sql, values=(), log_sql=True):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def closed(self):
        return self._closed

    def disconnect(self):
        self._closed = True
