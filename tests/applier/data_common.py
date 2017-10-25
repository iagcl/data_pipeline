import collections

TestCase = collections.namedtuple('TestCase', "description input_table_name input_commit_statements input_record_types input_operation_codes input_record_counts input_primary_key_fields input_commit_lsns expect_sql_execute_called expect_execute_called_times expect_audit_db_execute_sql_called expect_commit_called_times expect_batch_committed expect_insert_row_count expect_update_row_count expect_delete_row_count expect_source_row_count")

UPDATE_SSP_SQL = """
        UPDATE ctl.source_system_profile
        SET last_process_code = %s, max_lsn = %s
        WHERE 1 = 1
          AND profile_name         = %s
          AND version              = %s
          AND LOWER(target_region) = LOWER(%s)
          AND LOWER(object_name)   = LOWER(%s)"""
