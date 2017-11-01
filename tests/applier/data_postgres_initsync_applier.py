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

TestCase = collections.namedtuple('TestCase', "description input_table_name input_payloads input_record_types input_record_counts expect_copy_called_times expect_commit_called_times expect_batch_committed")

tests=[
  TestCase(
    description="Single record, no end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_record_types=[const.START_OF_BATCH, const.DATA],
    input_payloads=['', "1,albert"],
    input_record_counts=[0, 0],
    expect_batch_committed=[False, False],
    expect_copy_called_times=0,
    expect_commit_called_times=0
  )

, TestCase(
    description="Start of batch record only",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_record_types=[const.START_OF_BATCH],
    input_payloads=[''],
    input_record_counts=[0],
    expect_batch_committed=[False],
    expect_copy_called_times=0,
    expect_commit_called_times=0
  )

, TestCase(
    description="Single record with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_payloads=['',
                    '1,albert',
                    ''
    ],
    input_record_counts=[0, 0, 1],
    expect_batch_committed=[False, False, True],
    expect_copy_called_times=1,
    expect_commit_called_times=1
  )


]
