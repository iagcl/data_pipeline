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
# Module:    oracle_message
# Purpose:   Utility to define an oracle kafka message in python object and
#            provide methods to serialise and deserialise the message
#
# Notes:
#
###############################################################################

from .base_message import BaseMessage


class FileStreamMessage(BaseMessage):
    """Represents the contents of a line/record from a data file
    """
    def __init__(self):
        super(FileStreamMessage, self).__init__()

    def reset(self):
        super(FileStreamMessage, self).reset()
        self.payload = const.EMPTY_STRING
