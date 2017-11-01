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
import data_pipeline.processor.factory as processor_factory
from data_pipeline.db.exceptions import UnsupportedDbTypeError


def test_build_oracle_processor():
    processor = processor_factory.build(const.ORACLE)
    assert type(processor).__name__ == 'OracleCdcProcessor'

def test_build_mssql_processor():
    processor = processor_factory.build(const.MSSQL)
    assert type(processor).__name__ == 'MssqlCdcProcessor'

def test_build_unsupported():
    with pytest.raises(UnsupportedDbTypeError):
        processor = processor_factory.build("AnUnsupportedDatabase")
