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
# Notes:     Example to convert python message object:
#            1) initiate the object
#            myobject = OracleMessage()
#            2) populate the object such as:
#            myobject.operation_code = 'DELETE'
#            myobject.table_name = 'mytable'
#            myobject.commit_statement = 'DELETE FROM mytable WHERE id=5'
#            myobject.statement_id = '123456789'
#            myobject.commit_lsn = '10'
#            myobject.commit_timestamp = '01/12/2016 10:00:00:000'
#            myobject.message_sequence = '101'
#            myobject.multiline_flag = '0'
#            myobject.primary_key_fields = 'id'
#            3) call the tojaon method to convert object to a dict
#            mydict = myobject.serialise()
#
#            Example to convert dict message to oracle object:
#            1) initiate the object
#            myoracle = OracleMessage()
#            2) call the deserialise method to convert dict to oracle message
#            myoracle.deserialise(mydict)
#            3) access the message elements
#            mystatement = myoracle.commit_statement
#
###############################################################################

import data_pipeline.constants.const as const

from .base_message import BaseMessage


class OracleMessage(BaseMessage):
    def __init__(self):
        """Constructer OracleMessage()
        Attributes:
            operation_code (str): operation code
            statement_id (str): statement id
            commit_timestamp (str): commit timestamp
            message_sequence (str): message_sequence
            multiline_flag (str): multiline_flag
            commit_statement (str): commit statement
            primary_key_fields (str): primary key fields
        """
        super(OracleMessage, self).__init__()

    def reset(self):
        super(OracleMessage, self).reset()
        self.operation_code = const.EMPTY_STRING
        self.statement_id = const.EMPTY_STRING
        self.commit_timestamp = const.EMPTY_STRING
        self.multiline_flag = const.EMPTY_STRING
        self.commit_statement = const.EMPTY_STRING
        self.primary_key_fields = const.EMPTY_STRING
