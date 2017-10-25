import data_pipeline.constants.const as const
from .data_common import TestCase, UPDATE_SSP_SQL


tests=[

TestCase(
    description="Apply insert statement with metacols",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_commit_statements=[
        '',
        """insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COL_NAS_9") values ('26','string')""", 
        ''], 
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None, 
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COMPNDPK_1, CTL_INS_TS, CTL_UPD_TS ) VALUES ( 'string', '26', (SELECT CURRENT_TIMESTAMP), (SELECT CURRENT_TIMESTAMP) ); -- lsn: 0, offset: 1", 
        None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 1, 1],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]

  )


, TestCase(
    description="Apply update statement with metacols",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_commit_statements=[
        '', 
        """update "SYS"."CONNCT_CDC_PK5_COLS10" set "COL_V_2" = 'string' where "COMPNDPK_1" = \'26\'""", 
        ''], 
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None, 
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = 'string', CTL_UPD_TS = (SELECT CURRENT_TIMESTAMP) WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1"],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 1, 1],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]

  )


, TestCase(
    description="Apply insert and update statement with metacols",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_commit_statements=[
        '',
        """insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COL_NAS_9") values ('26','string')""", 
        '', 
        '', 
        """update "SYS"."CONNCT_CDC_PK5_COLS10" set "COL_V_2" = 'string' where "COMPNDPK_1" = \'26\'""", 
        ''], 
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH, const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, '', '', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1, 0, 0, 1],
    input_commit_lsns=[0, 0, 0, 0, 0, 0],
    expect_sql_execute_called=[
        None, 
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COMPNDPK_1, CTL_INS_TS, CTL_UPD_TS ) VALUES ( 'string', '26', (SELECT CURRENT_TIMESTAMP), (SELECT CURRENT_TIMESTAMP) ); -- lsn: 0, offset: 1", 
        None, 
        None, 
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = 'string', CTL_UPD_TS = (SELECT CURRENT_TIMESTAMP) WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1"],
    expect_execute_called_times=[0, 1, 1, 1, 2, 2],
    expect_audit_db_execute_sql_called=[None, None, None, None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1, 1, 1, 2],
    expect_insert_row_count=[0, 1, 1, 1, 1, 1],
    expect_update_row_count=[0, 0, 0, 0, 1, 1],
    expect_delete_row_count=[0, 0, 0, 0, 0, 0],
    expect_source_row_count=[0, 1, 1, 1, 2, 2],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]

  )

]
