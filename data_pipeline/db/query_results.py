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
# Module:    query_results
# Purpose:   An interface definition for retrieving query results
#
# Notes:
###############################################################################

from abc import ABCMeta, abstractmethod


class QueryResults(object):
    ___metaclass__ = ABCMeta

    def __init__(self):
        pass

    def __iter__(self):
        return self

    @abstractmethod
    def next(self):
        pass

    @abstractmethod
    def fetchone(self):
        pass

    @abstractmethod
    def fetchall(self):
        pass

    @abstractmethod
    def fetchmany(self, arraysize=None):
        pass
