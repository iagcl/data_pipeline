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
from .data_common import TestCase, UPDATE_SSP_SQL


tests=[
  TestCase(
    description="Skip insert and apply update statements belonging to inactive tables each with end of batch",
    input_table_name="INACTIVE_TABLE", 
    input_commit_statements=[
        '',
        'insert into "SYS"."INACTIVE_TABLE"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'26_varchar_1\',\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')', 
        '',
        '',
        'update "SYS"."INACTIVE_TABLE" set "COL_V_2" = \'26.9\' where "COMPNDPK_1" = \'26\'', 
        ''], 
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH, const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, '', '', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1, 0, 0, 1],
    input_commit_lsns=[0, 0, 0, 0, 0, 0],
    expect_sql_execute_called=[
        None, 
        None,
        None,
        None, 
        None,
        None],
    expect_execute_called_times=[0, 0, 0, 0, 0, 0],
    expect_audit_db_execute_sql_called=[None, None, None, None, None, None],
    expect_commit_called_times=[0, 0, 0, 0, 0, 0],
    expect_insert_row_count=[0, 0, 0, 0, 0, 0],
    expect_update_row_count=[0, 0, 0, 0, 0, 0],
    expect_delete_row_count=[0, 0, 0, 0, 0, 0],
    expect_source_row_count=[0, 0, 0, 0, 0, 0],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.UNCOMMITTED,]

  )

, TestCase(
    description="Don't skip insert and apply update statements not belonging to inactive tables each with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'26_varchar_1\',\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')', 
        '',
        '', 
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "COL_V_2" = \'26.9\' where "COMPNDPK_1" = \'26\'', 
        ''], 
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH, const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, '', '', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1, 0, 0, 1],
    input_commit_lsns=[0, 0, 0, 0, 0, 0],
    expect_sql_execute_called=[
        None, 
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COL_N_5, COL_N_6, COL_N_7, COL_N_8, COL_N_9, COL_TS_0, COL_V_1, COL_V_2, COL_V_3, COL_V_4, COMPNDPK_1, COMPNDPK_2, COMPNDPK_3, COMPNDPK_4, COMPNDPK_5 ) VALUES ( 'This is a nasty string ??a??????????', '26.5', '26.6', '26.7', '26.8', '26.9', '2017-04-19 12:14:22', '26_varchar_1', '26_varchar_2', '26_varchar_3', '26_varchar_4', '26', '26.1', '26.2', '26.3', '26.4' ); -- lsn: 0, offset: 1",
        None,
        None, 
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = '26.9' WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1",
        None],
    expect_execute_called_times=[0, 1, 1, 1, 2, 2],
    expect_audit_db_execute_sql_called=[None, None, None, None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1, 1, 1, 2],
    expect_insert_row_count=[0, 1, 1, 1, 1, 1],
    expect_update_row_count=[0, 0, 0, 0, 1, 1],
    expect_delete_row_count=[0, 0, 0, 0, 0, 0],
    expect_source_row_count=[0, 1, 1, 1, 2, 2],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED,]

  )

]
