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
import data_pipeline.constants.const as const

TARGETSCHEMA = 'ctl'

TestCase = collections.namedtuple('TestCase', "description input_table_name input_payloads input_record_types expect_error_message")

tests=[
  TestCase(
    description="End of batch without start",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_payloads=[''],
    input_record_types=[const.END_OF_BATCH],
    expect_error_message=["END_OF_BATCH received while already ended"]
  )

, TestCase(
    description="End of batch without start, followed by data",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_payloads=[
        '', 
        '1,albert'
    ], 
    input_record_types=[const.END_OF_BATCH, const.DATA],
    expect_error_message=[
        "END_OF_BATCH received while already ended", 
        "DATA message received without receiving START_OF_BATCH"]
  )

, TestCase(
    description="Start of batch after already started",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_payloads=['', ''],
    input_record_types=[const.START_OF_BATCH, const.START_OF_BATCH],
    expect_error_message=[None, "START_OF_BATCH received while already started"]
  )

, TestCase(
    description="Second end of batch after valid batch",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_payloads=['', 
        '1,albert', 
        '', 
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH, const.END_OF_BATCH],
    expect_error_message=[None, None, None, "END_OF_BATCH received while already ended"]
  )

]
