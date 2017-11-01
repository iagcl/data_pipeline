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
# Module:    base_message
# Purpose:   A common message class containing logic for serialising and
#            deserialising messages
#
###############################################################################

import data_pipeline.constants.const as const

from abc import ABCMeta, abstractmethod


class BaseMessage(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.reset()

    def reset(self):
        self.batch_id = const.EMPTY_STRING
        self.record_type = const.EMPTY_STRING
        self.record_count = 0
        self.table_name = const.EMPTY_STRING
        self.commit_lsn = const.EMPTY_STRING
        self.message_sequence = const.EMPTY_STRING

    def serialise(self):
        """Serialises this python object to a dictionary
        :return: Returns a dictionary representation of this object
        :rtype: dict
        """
        return self.__dict__

    def deserialise(self, message):
        """Applies the message to the attributes of this object
        :param dict message: input message in as a dict
        """
        self.reset()
        self.__dict__ = message

    def __str__(self):
        return str(self.serialise())
