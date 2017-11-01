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
# Module:    file_reader
# Purpose:   Utility to read a file of newline separated records, passing each
#            record out to an applier
#
# Notes:
#
###############################################################################

import ast
import bz2
import gzip
import logging

import data_pipeline.constants.const as const
import data_pipeline.utils.filesystem as fsutils

from data_pipeline.stream.filestream_message import FileStreamMessage


class FileReader(object):
    def __init__(self, filename):
        self._logger = logging.getLogger(__name__)
        self.handle = None
        self._filename = filename
        self._init_file_handle(filename)
        self._pc = None

    def _init_file_handle(self, filename):
        if not filename:
            error_message = "Input filename was not defined"
            self._report_error(error_message)
            raise ValueError()

        self.handle = fsutils.open_file(filename, 'r')
        self._logger.info("Opened file for reading: {filename}"
                          .format(filename=filename))

    def __iter__(self):
        return self

    def next(self):
        return self.handle.next()

    def readline(self):
        return self.handle.readline()

    def read_to(self, applier):
        self._pc = applier.process_control

        if self.handle is None:
            error_message = "Input file is not open"
            self._report_error(error_message)
            raise ValueError(error_message)

        self._logger.info("Reading input file '{}'"
                          .format(self._filename))
        for line in self.handle:
            self._logger.debug("Eval: {line}".format(line=line))
            message = ast.literal_eval(line)
            applier.apply(message)

        self._logger.info("Finished reading input file")

    def _report_error(self, error_message):
        if self._pc is not None:
            self._pc.comment = error_message
            self._pc.status = const.ERROR
            self._pc.update()
        self._logger.exception(error_message)

    def close(self):
        self.handle.close()

    def __del__(self):
        self.close()
