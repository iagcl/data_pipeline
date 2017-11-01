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
# Module:    cdc_processor
# Purpose:   Processes CDCs polled from Kafka queue
#
# Notes:
#
###############################################################################

import logging

from abc import ABCMeta, abstractmethod


class Processor(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self._set_logger()

    def _set_logger(self):
        self._logger = logging.getLogger(__name__)

    def renew_workdirectory(self):
        self._set_logger()

    @abstractmethod
    def deserialise(self):
        pass

    @abstractmethod
    def process(self, stream_message):
        """Process CDC messsage into a statement
        :param dict stream_message: Stream message payload polled from queue
        :return: Statement object representing the statement to apply to target
        :rtype: Statement
        """
        pass
