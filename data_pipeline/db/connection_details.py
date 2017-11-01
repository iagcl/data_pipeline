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
# Module:    connection_details
# Purpose:   Contains all details necessary for establishing a database
#            connection
#
# Notes:
###############################################################################


class ConnectionDetails:
    def __init__(self, userid=None, password=None, host=None, port=None,
                 dbsid=None, connect_timeout=10, charset='utf8', as_dict=False,
                 sslmode=None, sslcert=None, sslrootcert=None, sslkey=None,
                 sslcrl=None, data_dir=None):
        self.userid = userid
        self.password = password
        self.host = host
        self.port = port
        self.dbsid = dbsid
        self.connect_timeout = connect_timeout
        self.charset = charset
        self.as_dict = as_dict
        self.sslmode = sslmode
        self.sslcert = sslcert
        self.sslrootcert = sslrootcert
        self.sslkey = sslkey
        self.sslcrl = sslcrl
        self.data_dir = data_dir

    def __eq__(self, other):
        return (self.userid == other.userid and
                self.password == other.password and
                self.host == other.host and
                self.port == other.port and
                self.dbsid == other.dbsid)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return str(self.__dict__)
