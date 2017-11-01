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
import data_pipeline.constants.const as const
import tests.unittest_utils as unittest_utils

from data_pipeline.initsync.factory import build

@pytest.fixture
def setup(mocker, tmpdir):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    yield (mockargv_config)


@pytest.mark.parametrize("dbtype", [
    (const.ORACLE),
    (const.POSTGRES),
    (const.GREENPLUM),
    (const.MSSQL),
])
def test_build(dbtype, mocker, setup):
    (mockargv_config) = setup
    mockargv = mocker.Mock(**mockargv_config)

    db = build(dbtype, mockargv, None)
    assert type(db).__name__.lower() == "{}db".format(dbtype)
