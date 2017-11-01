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
    description="Empty commit statement, no end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA],
    input_operation_codes=['', const.UPDATE],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0],
    input_commit_lsns=[0, 0],
    expect_sql_execute_called=[None, None],
    expect_execute_called_times=[0, 0],
    expect_audit_db_execute_sql_called=[None , None],
    expect_commit_called_times=[0, 0],
    expect_insert_row_count=[0, 0],
    expect_update_row_count=[0, 0],
    expect_delete_row_count=[0, 0],
    expect_source_row_count=[0, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED]
  )

,  TestCase(
    description="Empty commit statement, with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        '',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, None, None],
    expect_execute_called_times=[0, 0, 0],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )


,  TestCase(
    description="Single logminer redo statement, no end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'26_varchar_1\',\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')'],
    input_record_types=[const.START_OF_BATCH, const.DATA],
    input_operation_codes=['', const.INSERT],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0],
    input_commit_lsns=[0, 0],
    expect_sql_execute_called=[None, None],
    expect_execute_called_times=[0, 0],
    expect_audit_db_execute_sql_called=[None , None],
    expect_commit_called_times=[0, 0],
    expect_insert_row_count=[0, 0],
    expect_update_row_count=[0, 0],
    expect_delete_row_count=[0, 0],
    expect_source_row_count=[0, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED]
  )

, TestCase(
    description="Start of batch record only",
    input_table_name='',
    input_commit_statements=[''],
    input_record_types=[const.START_OF_BATCH],
    input_operation_codes=[''],
    input_primary_key_fields='',
    input_record_counts=[0],
    input_commit_lsns=[0],
    expect_sql_execute_called=[None],
    expect_execute_called_times=[0],
    expect_audit_db_execute_sql_called=[None],
    expect_commit_called_times=[0],
    expect_insert_row_count=[0],
    expect_update_row_count=[0],
    expect_delete_row_count=[0],
    expect_source_row_count=[0],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED]
  )

, TestCase(
    description="Apply logminer redo statement with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'26_varchar_1\',\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        None,
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COL_N_5, COL_N_6, COL_N_7, COL_N_8, COL_N_9, COL_TS_0, COL_V_1, COL_V_2, COL_V_3, COL_V_4, COMPNDPK_1, COMPNDPK_2, COMPNDPK_3, COMPNDPK_4, COMPNDPK_5 ) VALUES\n  ( 'This is a nasty string ??a??????????', '26.5', '26.6', '26.7', '26.8', '26.9', '2017-04-19 12:14:22', '26_varchar_1', '26_varchar_2', '26_varchar_3', '26_varchar_4', '26', '26.1', '26.2', '26.3', '26.4' ) -- lsn: 0, offset: 1\n; -- lsn: 0, offset: 1",
    ],
                                      
    expect_execute_called_times=[0, 0, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 1],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply logminer redo statement containing curly braces with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'String with curly braces {1}.\',\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        None,
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COL_N_5, COL_N_6, COL_N_7, COL_N_8, COL_N_9, COL_TS_0, COL_V_1, COL_V_2, COL_V_3, COL_V_4, COMPNDPK_1, COMPNDPK_2, COMPNDPK_3, COMPNDPK_4, COMPNDPK_5 ) VALUES\n  ( 'This is a nasty string ??a??????????', '26.5', '26.6', '26.7', '26.8', '26.9', '2017-04-19 12:14:22', 'String with curly braces {1}.', '26_varchar_2', '26_varchar_3', '26_varchar_4', '26', '26.1', '26.2', '26.3', '26.4' ) -- lsn: 0, offset: 1\n; -- lsn: 0, offset: 1",
    ],

    expect_execute_called_times=[0, 0, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 1],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply logminer redo statement containing parentheses in values with end of batch",
    input_table_name="MY_TABLE",
    input_commit_statements=[
        '',
        """insert into "MY_SCHEMA"."MY_TABLE"("COMPANY","SEQUENCE","MVYEAR","MVMAKE","MVMODEL","MVSERIES","MVBODY","MVENGTYP","MVENGCAP","MVEQUIP","STDEQUIP","MVTARE") values ('1','124049','2010','LOTUS','EVORA',NULL,'COUPE','FI','35','(2 SEAT)','ABB ABS AC  AL  AW1 CLR EBD EDL IM  LSW PM  PS  PW  RCD RS  SPS TC  TCS','0')""",
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, ''],
    input_primary_key_fields="COMPANY",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        None,
        "INSERT INTO ctl.MY_TABLE ( COMPANY, MVBODY, MVENGCAP, MVENGTYP, MVEQUIP, MVMAKE, MVMODEL, MVSERIES, MVTARE, MVYEAR, SEQUENCE, STDEQUIP ) VALUES\n  ( '1', 'COUPE', '35', 'FI', '(2 SEAT)', 'LOTUS', 'EVORA', NULL, '0', '2010', '124049', 'ABB ABS AC  AL  AW1 CLR EBD EDL IM  LSW PM  PS  PW  RCD RS  SPS TC  TCS' ) -- lsn: 0, offset: 1\n; -- lsn: 0, offset: 1",
    ],
                                      
    expect_execute_called_times=[0, 0, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'my_table'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 1],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )


, TestCase(
    description="Apply logminer redo statement containing percent char in values with end of batch",
    input_table_name="MY_TABLE",
    input_commit_statements=[
        '',
        """insert into "MY_SCHEMA"."MY_TABLE"("COMPANY","SEQUENCE","MVYEAR","MVMAKE","MVMODEL","MVSERIES","MVBODY","MVENGTYP","MVENGCAP","MVEQUIP","STDEQUIP","MVTARE") values ('1','124049','2010','LOTUS','EVORA',NULL,'COUPE','FI','35','20%','ABB ABS AC  AL  AW1 CLR EBD EDL IM  LSW PM  PS  PW  RCD RS  SPS TC  TCS','0'); -- lsn: 0, offset: 1""",
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, ''],
    input_primary_key_fields="COMPANY",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        None,
        "INSERT INTO ctl.MY_TABLE ( COMPANY, MVBODY, MVENGCAP, MVENGTYP, MVEQUIP, MVMAKE, MVMODEL, MVSERIES, MVTARE, MVYEAR, SEQUENCE, STDEQUIP ) VALUES\n  ( '1', 'COUPE', '35', 'FI', '20%%', 'LOTUS', 'EVORA', NULL, '0', '2010', '124049', 'ABB ABS AC  AL  AW1 CLR EBD EDL IM  LSW PM  PS  PW  RCD RS  SPS TC  TCS' ) -- lsn: 0, offset: 1\n; -- lsn: 0, offset: 1",
    ],
                                      
    expect_execute_called_times=[0, 0, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'my_table'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 1],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

]
