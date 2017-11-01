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
import importlib
import pytest
import json
import data_pipeline.constants.const as const
import tests.unittest_utils as utils

from pytest_mock import mocker
from data_pipeline.stream.filestream_message import FileStreamMessage
from data_pipeline.db.db import Db


def load_tests(name):
    # Load module which contains test data
    tests_module = importlib.import_module(name)
    # Tests are to be found in the variable `tests` of the module
    for test in tests_module.tests:
        yield test


def pytest_generate_tests(metafunc):
    """ This allows us to load tests from external files by
    parametrizing tests with each test case found in a data_X
    file """
    for fixture in metafunc.fixturenames:
        if fixture.startswith('data_'):
            # Load associated test data
            current_package = __name__.rpartition('.')[0]
            tests = load_tests("{}.{}".format(current_package, fixture))
            metafunc.parametrize(fixture, tests)

def build_mock_audit_factory(mocker):
    pc_config = {'insert.return_value': None, 'update.return_value': None}
    mock_pc = mocker.Mock(**pc_config)

    pcd_config = {'insert.return_value': None, 'update.return_value': None, 'insert_row_count': 0, 'update_row_count': 0, 'delete_row_count': 0}
    mock_pcd = mocker.Mock(**pcd_config)

    audit_factory_config = {'build_process_control.return_value': mock_pc, 'build_process_control_detail.return_value': mock_pcd}
    mock_audit_factory = mocker.Mock(**audit_factory_config)
    return mock_audit_factory

@pytest.fixture()
def setup(tmpdir, mocker):
    db_config = {'copy.return_value': None }
    mock_target_db = mocker.Mock(**db_config)

    p = tmpdir.mkdir("test_apply_sql_dir")
    mockargv_config = utils.get_default_argv_config(tmpdir)
    mock_argv = mocker.Mock(**mockargv_config)

    utils.mock_get_program_args(mocker, 'data_pipeline.applier.applier.get_program_args', mock_argv)
    utils.setup_logging(mock_argv.workdirectory)

    mock_audit_factory = build_mock_audit_factory(mocker)

    processor = InitSyncProcessor()
    yield(PostgresInitSyncApplier(processor, mock_target_db, mock_argv, mock_audit_factory), mock_target_db, mock_argv)

def execute_tests(postgres_applier, data, mocker, mock_target_db, mock_argv):
    print("Running test: '{}'".format(data.description))
    
    for record_type, payload, record_count, expect_batch_committed in zip(data.input_record_types, data.input_payloads, data.input_record_counts, data.expect_batch_committed):

        message = FileStreamMessage()
        message.record_type = record_type
        message.table_name = data.input_table_name
        message.payload = payload
        message.record_count = record_count

        config = {'value.return_value': message.serialise()}
        mock_message = mocker.Mock(**config)
        
        batch_committed = postgres_applier.apply(mock_message.value())

        f = open(mock_argv.outputfile, 'r')
        for line in f:
            print("initsync outputfile contents={}".format(line))

        assert batch_committed == expect_batch_committed

    assert mock_target_db.commit.call_count == data.expect_commit_called_times
    assert mock_target_db.copy.call_count == data.expect_copy_called_times
        
def execute_batch_state_tests(postgres_applier, data, mocker, mock_target_db):
    print("Running test: '{}'".format(data.description))
    
    for record_type, payload, expect_error_message in zip(data.input_record_types, data.input_payloads, data.expect_error_message):

        message = FileStreamMessage()
        message.record_type = record_type
        message.table_name = data.input_table_name
        message.payload = payload

        config = {'value.return_value': message.serialise()}
        mock_message = mocker.Mock(**config)
        
        if expect_error_message:
            with pytest.raises(Exception) as err:
                postgres_applier.apply(mock_message.value())
                assert expect_error_message in str(err) 
