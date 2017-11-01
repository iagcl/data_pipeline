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

TestCase = collections.namedtuple('TestCase', "description input_table_name input_commit_statements input_record_types input_operation_codes input_record_counts input_primary_key_fields input_commit_lsns expect_sql_execute_called expect_execute_called_times expect_audit_db_execute_sql_called expect_commit_called_times expect_batch_committed expect_insert_row_count expect_update_row_count expect_delete_row_count expect_source_row_count")

UPDATE_SSP_SQL = """
        UPDATE ctl.source_system_profile
        SET last_process_code = %s, max_lsn = %s
        WHERE 1 = 1
          AND profile_name         = %s
          AND version              = %s
          AND LOWER(target_region) = LOWER(%s)
          AND LOWER(object_name)   = LOWER(%s)"""
