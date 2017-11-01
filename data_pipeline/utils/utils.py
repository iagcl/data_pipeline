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
###############################################################################
# Module:    utils
# Purpose:   Contains useful utility functions that don't belong to a category
#
# Notes:
#
###############################################################################


def dump(obj):
    s = ""
    for attr in dir(obj):
        if not attr.startswith("__"):
            s += "obj.%s = %s\n" % (attr, getattr(obj, attr))

    return s


def merge_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy."""
    if not y:
        return x

    z = x.copy()
    z.update(y)
    return z


def merge_attributes(obj, attr):
    """Given an object objA and an object objB, merge the
    dictionary of attributes from objB into objA. For example:

    obj is an instance of class A which has only one attribute, a:
        objA.a = 'foo'

    attr is a dictionary with a single key 'b':
        attr = { 'b': 'bar' }

        obj = merge_attributes(obj, attr)

    Now obj has two attributes: a and b
        obj.a = 'foo'
        obj.b = 'bar'

    Note that if attr contains keys on attributes already
    existing in obj, the values for these attributes on
    obj will be overwritten. For example:
        objA.a = 'foo'
        attr = { 'a': 'bar' }

        obj = merge_attributes(obj, attr)

    Now obj still has one attribute: a
        obj.a = 'bar'

    :param object obj: The object to merge attributes into, from attr
    :param dict attr: The dictionary to merge attributes from, into obj
    """

    obj.__dict__ = merge_dicts(obj.__dict__, attr)
