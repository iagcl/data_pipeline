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
import data_pipeline.constants.const as const
import data_postgres_cdc_applier 
from .data_common import TestCase, UPDATE_SSP_SQL


tests = [

 TestCase(
    description="Apply update statement containing a single primary key SET with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "COMPNDPK_1" = \'0\' where "COMPNDPK_1" = \'26\'',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        None,
        None],
    expect_execute_called_times=[0, 0, 0],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 1, 1],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED,]

  )

, TestCase(
    description="Apply update statement containing a primary key and non-primary key in SET with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "COMPNDPK_1" = \'0\', "COL_V_2" = \'26.9\' where "COMPNDPK_1" = \'26\'',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = '26.9' WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1",
        None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 1, 1],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED,]

  )

]
