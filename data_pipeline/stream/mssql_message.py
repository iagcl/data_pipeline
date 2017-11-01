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
# Module:    mssql_message
# Purpose:   Utility to define an mssql kafka message in python object and
#            provide methods to serialise and deserialise the message
#
# Notes:     Example to convert python message object:
#            1) initiate the object
#            myobject = MssqlMessage()
#            2) populate the object such as:
#            myobject.operation_type = '1'
#            myobject.operation_code = 'UPDATE'
#            myobject.table_name = 'mytable'
#            myobject.statement_id = '123456789'
#            myobject.commit_lsn = '10'
#            myobject.commit_timestamp = '01/12/2016 10:00:00:000'
#            myobject.message_sequence = '101'
#            myobject.column_names = 'id,name,lastname,address'
#            myobject.column_values = '100,Sam,Citizen,Sydney'
#            myobject.primary_key_fields = 'id'
#            3) call the serialise method to convert object to a dict
#            mydict = myobject.serialise()
#
#            Example to convert dict message to mssql object:
#            1) initiate the object
#            mymssql = MssqlMessage()
#            2) call the deserialise method to convert dict to mssql message
#            mymssql.deserialise(mydict)
#            3) access the message elements
#            mystatement = mymssql.commit_statement
#
###############################################################################

import data_pipeline.constants.const as const

from .base_message import BaseMessage


class MssqlMessage(BaseMessage):
    def __init__(self):
        """Constructer MssqlMessage()
        Attributes:
            operation_type (str): operation type
            operation_code (str): operation code
            statement_id (str): statement id
            commit_lsn (str): commit lsn
            commit_timestamp (str): commit timestamp
            message_sequence (str): message_sequence
            column_names (str): column names
            column_values (str): column values
            primary_key_fields (str): primary key fields
        """
        super(MssqlMessage, self).__init__()

    def reset(self):
        super(MssqlMessage, self).reset()
        self.operation_type = const.EMPTY_STRING
        self.operation_code = const.EMPTY_STRING
        self.statement_id = const.EMPTY_STRING
        self.commit_lsn = const.EMPTY_STRING
        self.commit_timestamp = const.EMPTY_STRING
        self.column_names = const.EMPTY_STRING
        self.column_values = const.EMPTY_STRING
        self.primary_key_fields = const.EMPTY_STRING
