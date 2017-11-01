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
# Module:    ddl_statement
# Purpose:   Parent class for DDL (Data Definition Language) statements
#
# Notes:
#
###############################################################################

import data_pipeline.constants.const as const

from abc import ABCMeta, abstractmethod
from .base_statement import BaseStatement


class DdlStatement(BaseStatement):
    """Contains data necessary for producing a valid DDL statement"""

    __metaclass__ = ABCMeta

    def __init__(self, table_name):
        super(DdlStatement, self).__init__(table_name)
        self._entries = []

    @property
    def entries(self):
        return self._entries

    @abstractmethod
    def add_entry(self, **kwargs):
        pass

    def _build_field_params(self, params):
        if params:
            return "({})".format(const.COMMASPACE.join(params))
        return const.EMPTY_STRING

    def _build_field_string(self, value):
        return " {}".format(value if value else const.EMPTY_STRING)
