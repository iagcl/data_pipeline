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
import collections
import data_pipeline.constants.const as const

TestCase = collections.namedtuple('TestCase', "input_dbtype expect_sql_call")

ORACLE_TABLE_EXISTS_SQL = """
        SELECT COUNT(*) AS CNT
        FROM ALL_OBJECTS
        WHERE OBJECT_TYPE IN ('TABLE','VIEW')
          AND OWNER       = UPPER('myschema')
          AND OBJECT_NAME = UPPER('mytable')"""


POSTGRES_TABLE_EXISTS_SQL = """
        SELECT EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_schema = 'myschema'
              AND table_name   = 'mytable'
        )"""


MSSQL_TABLE_EXISTS_SQL = """
        IF EXISTS (
          SELECT * FROM INFORMATION_SCHEMA.TABLES
          WHERE LOWER([table_schema]) = LOWER('myschema')
          AND   LOWER([table_name]) = LOWER('mytable')
        )
          BEGIN
            SELECT 1
          END
        ELSE
          BEGIN
            SELECT 0
          END"""

tests=[

    TestCase(
        input_dbtype=const.GREENPLUM,
        expect_sql_call=POSTGRES_TABLE_EXISTS_SQL,
    ),

    TestCase(
        input_dbtype=const.POSTGRES,
        expect_sql_call=POSTGRES_TABLE_EXISTS_SQL,
    ),

    TestCase(
        input_dbtype=const.ORACLE,
        expect_sql_call=ORACLE_TABLE_EXISTS_SQL,
    ),

    TestCase(
        input_dbtype=const.MSSQL,
        expect_sql_call=MSSQL_TABLE_EXISTS_SQL,
    ),
]
