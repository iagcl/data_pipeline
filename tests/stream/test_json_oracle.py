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
from data_pipeline.stream.oracle_message import OracleMessage

def test_reset():
    m = OracleMessage()
    assert m.operation_code == ''
    assert m.table_name == ''
    assert m.statement_id == ''
    assert m.commit_lsn == ''
    assert m.commit_timestamp == ''
    assert m.message_sequence == ''
    assert m.multiline_flag == ''
    assert m.commit_statement == ''
    assert m.primary_key_fields == ''

    m.operation_code = 'opcode'
    m.table_name = 'tablename'

    assert m.operation_code == 'opcode'
    assert m.table_name == 'tablename'
    assert m.statement_id == ''
    assert m.commit_lsn == ''
    assert m.commit_timestamp == ''
    assert m.message_sequence == ''
    assert m.multiline_flag == ''
    assert m.commit_statement == ''
    assert m.primary_key_fields == ''

    m.reset()

    assert m.operation_code == ''
    assert m.table_name == ''
    assert m.statement_id == ''
    assert m.commit_lsn == ''
    assert m.commit_timestamp == ''
    assert m.message_sequence == ''
    assert m.multiline_flag == ''
    assert m.commit_statement == ''
    assert m.primary_key_fields == ''


def test_deserialise_reset():
    m = OracleMessage()
    m.operation_code = 'opcode'
    m.table_name = 'tablename'
    m.multiline_flag = '1'

    assert m.operation_code == 'opcode'
    assert m.table_name == 'tablename'
    assert m.statement_id == ''
    assert m.commit_lsn == ''
    assert m.commit_timestamp == ''
    assert m.message_sequence == ''
    assert m.multiline_flag == '1'
    assert m.commit_statement == ''
    assert m.primary_key_fields == ''

    message = {  
            'operation_code': 'newopcode'
            , 'table_name': 'newtablename'
            , 'statement_id': 'an_id'
            , 'commit_lsn': ''
            , 'commit_timestamp': ''
            , 'message_sequence': ''
            , 'multiline_flag': '0'
            , 'commit_statement': ''
            , 'primary_key_fields': ''
    }

    m.deserialise(message)

    assert m.operation_code == 'newopcode'
    assert m.table_name == 'newtablename'
    assert m.statement_id == 'an_id'
    assert m.commit_lsn == ''
    assert m.commit_timestamp == ''
    assert m.message_sequence == ''
    assert m.multiline_flag == '0'
    assert m.commit_statement == ''
    assert m.primary_key_fields == ''
