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
from data_pipeline.sql.insert_statement import InsertStatement
from data_pipeline.sql.update_statement import UpdateStatement
from data_pipeline.sql.delete_statement import DeleteStatement

@pytest.fixture
def setup():
    insert_statement = InsertStatement("table_name", {'field_a': 'value_a', 'field_b': 'value_b', '/field_c\\': '/value_c\\'})
    yield (insert_statement)

@pytest.mark.parametrize("field_name, expected_value", [
    ("field_a", "value_a"),
    ("no_such_field", None),
    ("/field_c\\", "/value_c\\"),
    (None, None)])
def test_insert_get_value(field_name, expected_value, setup):
    insert_statement = setup
    val = insert_statement.get_value(field_name)
    assert val == expected_value


def test_insert_get_fields(setup):
    insert_statement = setup
    fields = insert_statement.get_fields()
    fields.sort()
    assert fields == ['/field_c\\', 'field_a', 'field_b']


@pytest.mark.parametrize("field_name, expected_value", [
    ("field_a", True),
    ("/field_c\\", True),
    ("no_such_field", False),
    (None, False)])
def test_insert_contains_field(field_name, expected_value, setup):
    insert_statement = setup
    val = insert_statement.contains_field(field_name)
    assert val == expected_value


def test_insert_str(setup):
    insert_statement = setup
    assert str(insert_statement) == "INSERT INTO table_name ( /field_c\\, field_a, field_b ) VALUES ( '/value_c\\\\', 'value_a', 'value_b' )"
