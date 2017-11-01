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

TestCase = collections.namedtuple('TestCase', "description input_table_name input_commit_statement input_primary_key_fields expected_entries expected_sql")

tests=[
 TestCase(
    description="Rejected",
    input_table_name="ALS2", 
    input_commit_statement="""ALTER TABLE ALS2 shrink space check""", 
    input_primary_key_fields=None,
    expected_entries=[],
    expected_sql=''
  )
]
