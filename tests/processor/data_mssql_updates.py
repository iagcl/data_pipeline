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

TestCase = collections.namedtuple('TestCase', "description input_table_name input_column_names input_column_values input_primary_key_fields expected_set_values expected_sql")

tests=[
  TestCase(
    description="Simple",
    input_table_name="students", 
    input_column_names="a{delim}b{delim}c{delim}d{delim}e{delim}f{delim}student_id{delim}lastname{delim}firstname{delim}middle_name".format(delim=const.FIELD_DELIMITER),
    input_column_values="a{delim}b{delim}c{delim}d{delim}e{delim}f{delim}1{delim}tim{delim}tam{delim}sam".format(delim=const.FIELD_DELIMITER),
    input_primary_key_fields = "student_id",
    expected_set_values={'a': 'a', 'b': 'b', 'c': 'c', 'd': 'd', 'e': 'e', 'f': 'f', 'student_id': '1', 'lastname': 'tim', 'firstname': 'tam', 'middle_name': 'sam'}, 
    expected_sql="UPDATE students SET a = 'a', c = 'c', b = 'b', e = 'e', d = 'd', firstname = 'tam', f = 'f', student_id = '1', lastname = 'tim', middle_name = 'sam' WHERE student_id = '1'")
]

