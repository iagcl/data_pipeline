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
# Module:  factory
# Purpose: Builds object instances required for auditing
#
# Notes:
#
###############################################################################

import datetime
import data_pipeline.constants.const as const
import data_pipeline.utils.dbuser as dbuser
import data_pipeline.audit.connection_factory as audit_conn_factory
import data_pipeline.db.factory as db_factory

from contextlib import contextmanager
from data_pipeline.audit.audit_dao import (ProcessControl,
                                           ProcessControlDetail,
                                           SourceSystemProfile)


@contextmanager
def get_audit_db(argv):
    audit_conn_details = dbuser.get_dbuser_properties(argv.audituser)
    if audit_conn_details is None:
        yield None
        return

    db = db_factory.build(const.POSTGRES)
    db.connect(audit_conn_details)
    try:
        yield db
    finally:
        db.close()


class AuditFactory(object):

    def __init__(self, argv):
        self._argv = argv
        self._audit_conn_details = dbuser.get_dbuser_properties(
            self._argv.audituser)
        self.session = audit_conn_factory.build_session(
            self._audit_conn_details)

    def get_process_control(self, id):
        return self.session.query(ProcessControl).get(id)

    def build_process_control(self, mode):
        pc = ProcessControl(self.session, self._argv)
        self._init_common_process_control(pc, mode)

        if mode == const.CDCEXTRACT:
            self._init_extractor_process_control(pc)
        elif mode == const.CDCAPPLY:
            self._init_applier_process_control(pc)
        elif (mode == const.INITSYNC or
              mode == const.INITSYNCEXTRACT or
              mode == const.INITSYNCAPPLY):
            self._init_initsync_process_control(pc)
        else:
            raise ValueError("Unsupported mode: {}".format(mode))

        return pc

    def build_process_control_detail(self, parent):
        pcd = ProcessControlDetail(self.session, self._argv)
        self._init_process_control_detail(parent, pcd)

        return pcd

    def _init_process_control_detail(self, parent, pcd):
        pcd.comment = const.EMPTY_STRING
        pcd.process_code = parent.process_code
        pcd.status = const.IN_PROGRESS
        pcd.duration = 0
        pcd.object_name = const.EMPTY_STRING
        pcd.source_row_count = 0
        pcd.insert_row_count = 0
        pcd.update_row_count = 0
        pcd.delete_row_count = 0
        pcd.bad_row_count = 0
        pcd.alter_count = 0
        pcd.create_count = 0
        pcd.process_starttime = datetime.datetime.now()
        pcd.process_endtime = datetime.datetime.now()
        pcd.delta_starttime = datetime.datetime.now()
        pcd.delta_endtime = datetime.datetime.now()
        pcd.delta_startlsn = const.EMPTY_STRING
        pcd.delta_endlsn = const.EMPTY_STRING
        pcd.error_message = const.EMPTY_STRING
        pcd.query_condition = const.EMPTY_STRING

        parent.details.append(pcd)

    def _init_extractor_process_control(self, pc):
        pc.source_system_type = self._argv.sourcedbtype
        pc.source_region = self._argv.sourceschema

    def _init_applier_process_control(self, pc):
        pc.target_system = self._argv.profilename
        pc.target_system_type = self._argv.targetdbtype
        pc.target_region = self._argv.targetschema

    def _init_initsync_process_control(self, pc):
        pc.source_system_code = self._argv.sourcesystem
        pc.source_system_type = self._argv.sourcedbtype
        pc.source_region = self._argv.sourceschema

        pc.target_system = self._argv.profilename
        pc.target_region = self._argv.targetschema
        pc.target_system_type = self._argv.targetdbtype

    def _init_common_process_control(self, pc, mode):
        pc.profile_name = self._argv.profilename
        pc.profile_version = self._argv.profileversion
        pc.source_system_code = self._argv.sourcesystem
        pc.process_code = mode
        pc.process_name = mode
        pc.process_starttime = datetime.datetime.now()
        pc.process_endtime = datetime.datetime.now()
        pc.min_lsn = const.EMPTY_STRING
        pc.max_lsn = const.EMPTY_STRING
        pc.status = const.IN_PROGRESS
        pc.duration = 0
        pc.comment = const.EMPTY_STRING
        pc.filename = "{}".format(self._argv.outputfile)
        pc.executor_run_id = 0
        pc.executor_status = const.EMPTY_STRING
        pc.infolog = const.EMPTY_STRING
        pc.errorlog = const.EMPTY_STRING

    def __del__(self):
        self.session.close_all()
