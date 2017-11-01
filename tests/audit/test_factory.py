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
import tests.unittest_utils as unittest_utils
import data_pipeline.constants.const as const

from data_pipeline.audit.factory import AuditFactory, get_audit_db

@pytest.fixture()
def setup(tmpdir, mocker):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv = mocker.Mock(**mockargv_config)

    mocksession_config = {}
    mocksession = mocker.Mock(**mocksession_config)

    mock_build_session = mocker.patch('data_pipeline.audit.connection_factory.build_session')
    mock_build_session.return_value = mocksession
    yield (mockargv)


def test_build_process_control(setup):
    (mockargv) = setup

    audit_factory = AuditFactory(mockargv)
    mode = const.INITSYNC
    pc = audit_factory.build_process_control(mode)

    assert type(pc).__name__ == 'ProcessControl'


def test_get_audit_db(mocker, setup):
    (mockargv) = setup

    mock_db_config = { }
    mock_db = mocker.Mock(**mock_db_config)

    mock_db_factory = mocker.patch("data_pipeline.audit.factory.db_factory")
    mock_db_factory.build.return_value = mock_db

    with get_audit_db(mockargv) as db:
        print("db = {}".format(db))

        mock_db.connect.assert_called_once()
        mock_db_factory.build.assert_called_once_with(const.POSTGRES)
