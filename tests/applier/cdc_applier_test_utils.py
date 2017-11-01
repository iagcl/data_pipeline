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
import pytest
import importlib
import tests.unittest_utils as unittest_utils
import data_pipeline.utils.utils as utils
import data_pipeline.constants.const as const

from data_pipeline.stream.oracle_message import OracleMessage
from data_pipeline.processor.oracle_cdc_processor import OracleCdcProcessor
from data_pipeline.applier.applier import Applier
from data_pipeline.audit.audit_dao import ProcessControlDetail


class MockDbState(object):
    def __init__(self):
        self._connected = False

    def connect(self, conn_details):
        self._connected = True

    def closed(self):
        return not self._connected

    def close(self):
        self._connected = False


def setup_dependencies(tmpdir, mocker, metacols, inactive_applied_tables,
                       config_to_merge):
    if not inactive_applied_tables:
        inactive_applied_tables = set()

    mockdbstate = MockDbState()

    db_config = {'closed.side_effect': mockdbstate.closed,
                 'connect.side_effect': mockdbstate.connect,
                 'close.side_effect': mockdbstate.close}
    mock_target_db = mocker.Mock(**db_config)

    p = tmpdir.mkdir("test_apply_sql_dir")
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)

    mockargv_config = utils.merge_dicts(mockargv_config, config_to_merge)
    mockargv = mocker.Mock(**mockargv_config)

    unittest_utils.mock_get_program_args(
            mocker, 
            'data_pipeline.applier.applier.get_program_args',
            mockargv)
    unittest_utils.setup_logging(mockargv.workdirectory)

    unittest_utils.mock_get_inactive_applied_tables(
            mocker, inactive_applied_tables)

    mock_audit_factory = build_mock_audit_factory(mocker, mockargv)
    mock_audit_db = patch_db_factory(mocker)

    oracle_processor = OracleCdcProcessor(metacols)

    f = mocker.patch.object(Applier, '_get_max_lsn_source_system_profile')

    return (oracle_processor, mock_target_db,
            mockargv, mock_audit_factory, mock_audit_db)


def build_mock_audit_factory(mocker, mockargv):
    pc_config = {'insert.return_value': None, 'update.return_value': None}
    mock_pc = mocker.Mock(**pc_config)

    mock_pcd = ProcessControlDetail(None, mockargv)
    mock_pcd.insert_row_count = 0
    mock_pcd.update_row_count = 0
    mock_pcd.delete_row_count = 0
    mock_pcd.source_row_count = 0
    mock_pcd.alter_count = 0
    mock_pcd.create_count = 0

    audit_factory_config = {
        'build_process_control.return_value': mock_pc,
        'build_process_control_detail.return_value': mock_pcd
    }
    mock_audit_factory = mocker.Mock(**audit_factory_config)
    return mock_audit_factory

def patch_db_factory(mocker):
    def execute_query_se(sql, arraysize, bind_variables):
        print("Query executed: {}".format(sql))
        if "SELECT executor_run_id, executor_status, status" in sql:
            executor_run_id = 99
            executor_status = const.SUCCESS
            status = const.SUCCESS
            mock_query_results_config = {
                'fetchone.return_value': (executor_run_id, executor_status, status)
            }
            return mocker.Mock(**mock_query_results_config)

        raise Exception("Query '{}' is not supported in mock!".format(sql))

    mock_db_config = {'execute_query.side_effect': execute_query_se}
    mock_db = mocker.Mock(**mock_db_config)

    mock_db_factory = mocker.patch("data_pipeline.audit.factory.db_factory")
    mock_db_factory.build.return_value = mock_db

    return mock_db

def execute_tests(applier, data, mocker, mock_target_db, mock_audit_db):
    print("Running test: '{}'".format(data.description))
    
    for (record_type,
         op,
         statement,
         record_count,
         lsn,
         expect_sql_execute_called,
         expect_execute_called_times,
         expect_audit_db_execute_sql_called,
         expect_commit_called_times,
         expect_insert_row_count,
         expect_update_row_count,
         expect_delete_row_count,
         expect_source_row_count,
         expect_batch_committed) in (

         zip(data.input_record_types,
             data.input_operation_codes,
             data.input_commit_statements,
             data.input_record_counts,
             data.input_commit_lsns,
             data.expect_sql_execute_called,
             data.expect_execute_called_times,
             data.expect_audit_db_execute_sql_called,
             data.expect_commit_called_times,
             data.expect_insert_row_count,
             data.expect_update_row_count,
             data.expect_delete_row_count,
             data.expect_source_row_count,
             data.expect_batch_committed)):

        oracle_message = OracleMessage()
        oracle_message.record_type = record_type
        oracle_message.operation_code = op
        oracle_message.table_name = data.input_table_name
        oracle_message.commit_statement = statement
        oracle_message.primary_key_fields = data.input_primary_key_fields
        oracle_message.record_count = record_count
        oracle_message.commit_lsn = lsn

        config = {'value.return_value': oracle_message.serialise(),
                  'offset.return_value': 1}
        mock_message = mocker.Mock(**config)
        
        commit_status = applier.apply(mock_message)

        print("mock_target_db.execute.call_count {} == expect_execute_called_times {}?".format(mock_target_db.execute.call_count, expect_execute_called_times))
        assert mock_target_db.execute.call_count == expect_execute_called_times

        print("mock_target_db.execute.call_count {} == expect_execute_called_times {}?".format(mock_target_db.execute.call_count, expect_execute_called_times))
        assert mock_target_db.execute.call_count == expect_execute_called_times

        print("mock_target_db.commit.call_count {} == expect_commit_called_times {}?".format(mock_target_db.commit.call_count, expect_commit_called_times))
        assert mock_target_db.commit.call_count == expect_commit_called_times

        print("commit_status {} == expect_batch_committed {}?".format(commit_status, expect_batch_committed))
        assert commit_status == expect_batch_committed

        pcd = applier.get_pcd(data.input_table_name)
        assert pcd is not None
        print("insert={}, update={}, delete={}, source={}"
              .format(pcd.insert_row_count,
                      pcd.update_row_count,
                      pcd.delete_row_count,
                      pcd.source_row_count))

        print("pcd.insert_row_count {} == expect_insert_row_count {}?"
              .format(pcd.insert_row_count, expect_insert_row_count))
        assert pcd.insert_row_count == expect_insert_row_count

        print("pcd.update_row_count {} == expect_update_row_count {}?"
              .format(pcd.update_row_count, expect_update_row_count))
        assert pcd.update_row_count == expect_update_row_count

        print("pcd.delete_row_count {} == expect_delete_row_count {}?"
              .format(pcd.delete_row_count, expect_delete_row_count))
        assert pcd.delete_row_count == expect_delete_row_count

        print("pcd.source_row_count {} == expect_source_row_count {}?"
              .format(pcd.source_row_count, expect_source_row_count))
        assert pcd.source_row_count == expect_source_row_count

        if expect_sql_execute_called:
            mock_target_db.execute.assert_called_with(
                expect_sql_execute_called)

        if expect_audit_db_execute_sql_called:
            (sql, bind_values) = expect_audit_db_execute_sql_called
            if sql:
                print("sql = ({}, {})".format(sql, bind_values))
                mock_audit_db.execute.assert_called_with(
                    sql, bind_values)


def execute_batch_state_tests(applier, data, mocker, mock_target_db):
    print("Running test: '{}'".format(data.description))
    
    for record_type, op, statement in (
        zip(data.input_record_types, data.input_operation_codes,
            data.input_commit_statements)):

        oracle_message = OracleMessage()
        oracle_message.record_type = record_type
        oracle_message.operation_code = op
        oracle_message.table_name = data.input_table_name
        oracle_message.commit_statement = statement
        oracle_message.primary_key_fields = data.input_primary_key_fields

        config = {'value.return_value': oracle_message.serialise(),
                  'offset.return_value': 1}
        mock_message = mocker.Mock(**config)
