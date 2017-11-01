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
from version import get_version
from pytest_mock import mocker


def test_get_version(mocker):
    mock_check_output = mocker.patch("version.check_output")
    mock_check_output.return_value = "v0.4.2-32-ga2ccd08"
    version = get_version()
    assert version == "0.4.2.post32"


def test_get_version(mocker):
    mock_check_output = mocker.patch("version.check_output")
    mock_check_output.return_value = "v0.4.2-32-ga2ccd08-dirty"
    version = get_version()
    assert version == "0.4.2.post32*"
