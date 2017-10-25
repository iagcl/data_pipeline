import data_pipeline.constants.const as const
from .data_common import TestCase, UPDATE_SSP_SQL


tests=[
  TestCase(
    description="Skip insert and apply update statements each with end of batch",
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
        None,
        None,
        None, 
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = '26.9' WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1",
        None],
    expect_execute_called_times=[0, 0, 0, 0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, None, None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 0, 0, 0, 1],
    expect_insert_row_count=[0, 0, 0, 0, 0, 0],
    expect_update_row_count=[0, 0, 0, 0, 1, 1],
    expect_delete_row_count=[0, 0, 0, 0, 0, 0],
    expect_source_row_count=[0, 0, 0, 0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED,]

  )
]
