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
# Purpose: Build concrete instances of specific database clients
#
# Notes:
#
###############################################################################

from .exceptions import UnsupportedDbTypeError
import data_pipeline.constants.const as const


def build(dbtype_name):
    """Return the specific type of db object given the dbtype_name"""
    if dbtype_name == const.ORACLE:
        from .oracledb import OracleDb
        return OracleDb()
    elif dbtype_name == const.MSSQL:
        from .mssqldb import MssqlDb
        return MssqlDb()
    elif dbtype_name == const.POSTGRES:
        from .postgresdb import PostgresDb
        return PostgresDb()
    elif dbtype_name == const.GREENPLUM:
        from .greenplumdb import GreenplumDb
        return GreenplumDb()
    elif dbtype_name == const.FILE:
        from .filedb import FileDb
        return FileDb()
    else:
        raise UnsupportedDbTypeError(dbtype_name)
