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
# Module:  factory
# Purpose: Build concrete instances of specific CDC processors
#
# Notes:
#
###############################################################################

import data_pipeline.constants.const as const

from data_pipeline.db.exceptions import UnsupportedDbTypeError
from data_pipeline.processor.oracle_cdc_processor import OracleCdcProcessor
from data_pipeline.processor.mssql_cdc_processor import MssqlCdcProcessor


def build(source_dbtype, metacols=None):
    """Return the specific type of CDC Processor object given the
    source_dbtype
    """
    if source_dbtype == const.ORACLE:
        return OracleCdcProcessor(metacols)
    elif source_dbtype == const.MSSQL:
        return MssqlCdcProcessor()
    else:
        raise UnsupportedDbTypeError(source_dbtype)
