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
import data_pipeline.utils.utils as utils


FOO = 'foo'
BAR = 'bar'
CAR = 'car'


@pytest.mark.parametrize("attr, expected_a, expected_b, expected_c", [
    ({'c': CAR}, FOO, BAR, CAR),
    ({'a': CAR}, CAR, BAR, None),
    ({'b': CAR, 'c': FOO}, FOO, CAR, FOO),
])
def test_merge_attributes(attr, expected_a, expected_b, expected_c):
    class A:
        def __init__(self):
            self.a = FOO
            self.b = BAR

    a = A()
    assert a.a == FOO
    assert a.b == BAR

    c = utils.merge_attributes(a, attr)

    # Assert nothing's changed on the original object
    assert a.a == expected_a
    assert a.b == expected_b
    if expected_c is not None:
        assert a.c == expected_c
