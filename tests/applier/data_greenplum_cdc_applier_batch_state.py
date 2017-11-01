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
import data_postgres_cdc_applier_batch_state

TARGETSCHEMA = 'ctl'

TestCase = collections.namedtuple('TestCase', "description input_table_name input_commit_statements input_record_types input_operation_codes input_primary_key_fields expect_error_message")

tests = data_postgres_cdc_applier_batch_state.tests
