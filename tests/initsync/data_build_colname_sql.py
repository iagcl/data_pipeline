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

TestCase = collections.namedtuple('TestCase', "input_dbtype input_is_source_db expect_sql_call expect_cols")

# Oracle Column Name SQL

ORACLE_SOURCE_COLNAME_SQL = """
        SELECT column_name, data_type
        FROM ALL_TAB_COLUMNS
        WHERE OWNER      = UPPER('myschema')
          AND TABLE_NAME = UPPER('mytable')
          AND DATA_TYPE NOT IN ('SDO_GEOMETRY', 'RAW', 'BLOB')
        ORDER BY COLUMN_ID"""


ORACLE_TARGET_COLNAME_SQL = """
        SELECT LOWER(column_name), data_type
        FROM ALL_TAB_COLUMNS
        WHERE OWNER      = UPPER('myschema')
          AND TABLE_NAME = UPPER('mytable')
          AND DATA_TYPE NOT IN ('SDO_GEOMETRY', 'RAW', 'BLOB')
        ORDER BY COLUMN_ID"""

# MSSQL Column Name SQL

MSSQL_SOURCE_COLNAME_SQL = """
        SELECT [column_name], [data_type]
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER([table_schema]) = LOWER('myschema')
          AND LOWER([table_name]) = LOWER('mytable')
          AND UPPER([data_type]) NOT IN ('IMAGE')
        ORDER BY ORDINAL_POSITION"""


MSSQL_TARGET_COLNAME_SQL = """
        SELECT LOWER([column_name]), [data_type]
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER([table_schema]) = LOWER('myschema')
          AND LOWER([table_name]) = LOWER('mytable')
          AND UPPER([data_type]) NOT IN ('IMAGE')
        ORDER BY ORDINAL_POSITION"""

# Postgres Column Name SQL

POSTGRES_SOURCE_COLNAME_SQL = """
        SELECT column_name, data_type
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('myschema')
          AND LOWER(table_name) = LOWER('mytable')
          AND UPPER(data_type) NOT IN ('IMAGE')
        ORDER BY ORDINAL_POSITION"""


POSTGRES_TARGET_COLNAME_SQL = """
        SELECT LOWER(column_name), data_type
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('myschema')
          AND LOWER(table_name) = LOWER('mytable')
          AND UPPER(data_type) NOT IN ('IMAGE')
        ORDER BY ORDINAL_POSITION"""


# Greenplum Column Name SQL

GREENPLUM_SOURCE_COLNAME_SQL = """
        SELECT column_name, data_type
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('myschema')
          AND LOWER(table_name) = LOWER('mytable')
          AND UPPER(data_type) NOT IN ('IMAGE')
        ORDER BY ORDINAL_POSITION"""


GREENPLUM_TARGET_COLNAME_SQL = """
        SELECT LOWER(column_name), data_type
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('myschema')
          AND LOWER(table_name) = LOWER('mytable')
          AND UPPER(data_type) NOT IN ('IMAGE')
        ORDER BY ORDINAL_POSITION"""

ORACLE_COL1_REPLACE_CONTROL_CHARS_SQL = """REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("col1", CHR(30), ''), CHR(29), ''), CHR(13), ''), CHR(10), ''), CHR(2), ''), CHR(0), '')"""

MSSQL_COL1_REPLACE_CONTROL_CHARS_SQL = """REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE([col1] COLLATE Latin1_General_BIN, CHAR(30), ''), CHAR(29), ''), CHAR(13), ''), CHAR(10), ''), CHAR(2), ''), CHAR(0), '')"""

POSTGRES_COL1_REPLACE_CONTROL_CHARS_SQL = """REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("col1", CHR(30), ''), CHR(29), ''), CHR(13), ''), CHR(10), ''), CHR(2), '')"""

GREEENPLUM_COL1_REPLACE_CONTROL_CHARS_SQL = POSTGRES_COL1_REPLACE_CONTROL_CHARS_SQL


tests=[

    TestCase(
        input_dbtype=const.ORACLE,
        input_is_source_db=True,
        expect_sql_call=ORACLE_SOURCE_COLNAME_SQL,
        expect_cols=[ORACLE_COL1_REPLACE_CONTROL_CHARS_SQL, '"/col2\\"', '"bitcol"']
    ),
    TestCase(
        input_dbtype=const.ORACLE,
        input_is_source_db=False,
        expect_sql_call=ORACLE_TARGET_COLNAME_SQL,
        expect_cols=['col1', '_col2_', 'bitcol']
    ),
    TestCase(
        input_dbtype=const.MSSQL,
        input_is_source_db=True,
        expect_sql_call=MSSQL_SOURCE_COLNAME_SQL,
        expect_cols=[MSSQL_COL1_REPLACE_CONTROL_CHARS_SQL,
                     '[/col2\\]',
                     'CAST([bitcol] AS TINYINT) AS [bitcol]']
    ),
    TestCase(
        input_dbtype=const.MSSQL,
        input_is_source_db=False,
        expect_sql_call=MSSQL_TARGET_COLNAME_SQL,
        expect_cols=['col1', '_col2_', 'bitcol']
    ),
    TestCase(
        input_dbtype=const.POSTGRES,
        input_is_source_db=True,
        expect_sql_call=POSTGRES_SOURCE_COLNAME_SQL,
        expect_cols=[POSTGRES_COL1_REPLACE_CONTROL_CHARS_SQL, '"/col2\\"', '"bitcol"']
    ),
    TestCase(
        input_dbtype=const.POSTGRES,
        input_is_source_db=False,
        expect_sql_call=POSTGRES_TARGET_COLNAME_SQL,
        expect_cols=['col1', '_col2_', 'bitcol']
    ),
    TestCase(
        input_dbtype=const.GREENPLUM,
        input_is_source_db=True,
        expect_sql_call=GREENPLUM_SOURCE_COLNAME_SQL,
        expect_cols=[GREEENPLUM_COL1_REPLACE_CONTROL_CHARS_SQL, '"/col2\\"', '"bitcol"']
    ),
    TestCase(
        input_dbtype=const.GREENPLUM,
        input_is_source_db=False,
        expect_sql_call=GREENPLUM_TARGET_COLNAME_SQL,
        expect_cols=['col1', '_col2_', 'bitcol']
    ),
]
