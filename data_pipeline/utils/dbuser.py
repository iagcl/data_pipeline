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
# Module:  dbuser
# Purpose: Split database user strings into database connection properties.
#          Refer to notes below. Database properties include:
#
#          - dbuserid
#          - dbpassword
#          - host (database name)
#          - dbport
#
#          OR
#
#          - file data directory
#
# Notes:
# (1) database user string format:
#
#     <dbuserid>/<dbpassword>@<host>[:<dbport>][/<dbsid>]
#
# (2) for file-base loads, the user string format is a valid directory path:
#
#     /path/to/data/files
#
# (3) For MacOS users, if you're seeing an error like the following after
#     being prompted for a password:
#
#     "keyring.errors.PasswordSetError: Can't store password on keychain"
#
#     Please try resigning the python executable as outlined here:
#     https://github.com/jaraco/keyring/issues/219
#
#     E.g. codesign -f -s - /path/to/my/virtualenv/bin/python
#
###############################################################################

import getpass
import keyring
import logging
import os
import re

from data_pipeline.db.connection_details import ConnectionDetails

logger = logging.getLogger(__name__)


def get_dbuser_properties(dbuser):
    if not dbuser:
        return None

    conn_details = _get_db_connection_properties(dbuser)

    if conn_details.data_dir:
        return conn_details

    if not conn_details.password:
        if not conn_details.host:
            raise ValueError("Invalid connection string: Host missing.")
        if not conn_details.userid:
            raise ValueError("Invalid connection string: User ID missing.")

        logger.info("Trying to fetch password from keyring conn_details: {}"
                    .format(conn_details))
        # Try to fetch the password from keyring
        system_key = ("{host}:{port}/{dbsid}"
                      .format(host=conn_details.host,
                              port=conn_details.port,
                              dbsid=conn_details.dbsid))

        password = keyring.get_password(system_key,
                                        conn_details.userid)
        password_confirm = password

        while password is None or password != password_confirm:
            password = getpass.getpass(
                "Password for {dbuser}: ".format(dbuser=dbuser))

            password_confirm = getpass.getpass(
                "Confirm password for {dbuser}: ".format(dbuser=dbuser))

            if password != password_confirm:
                logger.warn("Passwords do not match. Please try again...")

        # Store the password on the machine's keychain for later retrieval
        keyring.set_password(system_key,
                             conn_details.userid,
                             password)

        conn_details.password = password

    _validate(conn_details, dbuser)

    return conn_details


def _validate(conn_details, conn_string):
    if conn_details is None:
        raise ValueError("Connection details could not be defined with "
                         "connection string: {}".format(conn_string))

    if not conn_details.userid:
        raise ValueError("User ID was not defined in connection string: {}"
                         .format(conn_string))

    if not conn_details.password:
        raise ValueError("Password is not defined for connection string: {}"
                         .format(conn_string))

    if not conn_details.host:
        raise ValueError("Host is not defined in connection string: {}"
                         .format(conn_string))


def _get_dbuser(buf):
    fwd_slash_index = buf.find('/')
    if fwd_slash_index >= 0:
        return buf[:fwd_slash_index]
    return buf


def _get_dbpassword(buf):
    fwd_slash_index = buf.find('/')
    if fwd_slash_index >= 0:
        return buf[fwd_slash_index+1:]
    return None


def _get_host(buf):
    colon_index = buf.rfind(':')
    if colon_index >= 0:
        return buf[:colon_index]

    fwd_slash_index = buf.rfind('/')
    if fwd_slash_index >= 0:
        return buf[:fwd_slash_index]

    return buf


def _get_dbport(buf):
    colon_index = buf.rfind(':')
    if colon_index >= 0:
        fwd_slash_index = buf.rfind('/')

        if fwd_slash_index < 0:
            return buf[colon_index+1:]

        return buf[colon_index+1:fwd_slash_index]
    return None


def _get_dbsid(buf):
    fwd_slash_index = buf.rfind('/')
    if fwd_slash_index >= 0:
        return buf[fwd_slash_index+1:]
    return None


def _get_db_connection_properties(buf):
    last_at_index = buf.rfind('@')

    # Treat this as a data directory path
    if last_at_index < 0:
        if not os.path.isdir(buf):
            raise ValueError("No such directory: {d}".format(d=buf))
        if not os.listdir(buf):
            raise ValueError("Directory is empty: {d}".format(d=buf))

        return ConnectionDetails(data_dir=buf)

    user_password_part = buf[:last_at_index]
    userid = _get_dbuser(user_password_part)
    password = _get_dbpassword(user_password_part)

    rest = buf[last_at_index+1:]
    host = _get_host(rest)
    port = _get_dbport(rest)
    dbsid = _get_dbsid(rest)

    return ConnectionDetails(userid=userid, password=password,
                             host=host, port=port, dbsid=dbsid)
