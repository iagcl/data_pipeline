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
# Module:   custom_orm
# Purpose:  Module containing object relational models for audit tables
#           and associated helper methods
#
# Notes:    This module was created to address the lack of connection
#           resiliency, ease of use and extensibility in sqlalchemy.
#           At the time of writing this, this custom_orm module was used
#           specifically by initsync and duplicates the audit_dao module's
#           model, which is used by the CDC modules. There are plans to unify
#           these two modules in future.
#
###############################################################################

import datetime
import logging
import data_pipeline.constants.const as const
import data_pipeline.db.factory as dbfactory
import data_pipeline.utils.dbuser as dbuser

from abc import ABCMeta, abstractmethod
from data_pipeline.utils.utils import dump
from data_pipeline.sql.utils import TableName


def _add_connection_resiliency(func):
    def func_wrapper(self, **kwargs):
        if self._db is None:
            return func(self, **kwargs)

        attempts = 0
        exception = None
        while attempts < const.MAX_AUDIT_ATTEMPTS:
            try:
                self._connect()
                ret = func(self, **kwargs)
                self._db.commit()

                msg = ("Successful {funcname} of {table_name} "
                       "record with id = {id}"
                       .format(funcname=func.__name__,
                               table_name=self._table.name,
                               id=self.id))

                self._logger.debug(msg)
                break
            except Exception, e:
                msg = ("{funcname} failed on attempt #{numattempts}. "
                       "Reattempting..."
                       .format(funcname=func.__name__,
                               numattempts=attempts))

                self._logger.warn(msg)
                exception = e
                attempts += 1

        if attempts == const.MAX_AUDIT_ATTEMPTS:
            self._logger.exception("{funcname} failed. {error}"
                                   .format(funcname=func.__name__,
                                           error=str(e)))
            raise exception

        return ret

    return func_wrapper


class AuditBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, table_name, argv):
        self.id = 0
        self.argv = argv
        self._logger = logging.getLogger(__name__)
        self._audit_conn_details = dbuser.get_dbuser_properties(
            self.argv.audituser)

        self._db = None
        if self._audit_conn_details:
            self._db = dbfactory.build(const.POSTGRES)

        self._table = TableName(self.argv.auditschema, table_name)
        self._fieldnames = []
        self._additional_update_fields = []

    @_add_connection_resiliency
    def select(self, **kwargs):
        if self._db is None:
            return False
        return self._select(**kwargs)

    def _select(self, **kwargs):
        params = []
        where_entries = []

        for fieldname, value in kwargs.items():
            setattr(self, fieldname, value)
            where_entries.append("{fieldname} = %s"
                                 .format(fieldname=fieldname))
            params.append(value)

        where_string = const.EMPTY_STRING
        if where_entries:
            anded_where = "\n          AND ".join(where_entries)
            where_string = "AND {where}".format(where=anded_where)

        select_sql = """
        SELECT *
        FROM {table}
        WHERE 1=1
          {where}""".format(table=self._table.fullname, where=where_string)

        results = self._db.execute_query(select_sql,
                                         self.argv.arraysize,
                                         params)
        all_results = results.fetchall()

        if len(all_results) == 0 or len(all_results) > 1:
            return False

        r = all_results[0]
        colnames = results.get_col_names()
        for i in range(0, len(colnames)):
            setattr(self, colnames[i], r[i])

        return True

    @_add_connection_resiliency
    def insert(self, **kwargs):
        if self._db is None:
            return

        self._pre_insert()
        self._insert(**kwargs)
        self.id = self._db.cursor.fetchone()[0]
        if self.argv.veryverbose:
            self._logger.debug("insert(): {}".format(str(self)))

    @abstractmethod
    def _pre_insert(self):
        pass

    def _insert(self, **kwargs):
        field_names = ",\n            ".join(self._fieldnames)
        field_value_placeholders = ["%s" for attr in self._fieldnames]
        field_values = ",\n            ".join(field_value_placeholders)
        insert_sql = """
        INSERT INTO {table_name} (
            {field_names}
        ) VALUES (
            {field_values}
        ) RETURNING id""".format(table_name=self._table.fullname,
                                 field_names=field_names,
                                 field_values=field_values)

        for fieldname, value in kwargs.items():
            setattr(self, fieldname, value)

        bind_values = [getattr(self, fieldname)
                       for fieldname in self._fieldnames]
        self._db.execute(insert_sql, bind_values, log_sql=self.argv.verbose)

    @_add_connection_resiliency
    def update(self, **kwargs):
        if self._db is None:
            return

        self._pre_update()
        self._update(**kwargs)
        if self.argv.veryverbose:
            self._logger.debug("update(): {}".format(self))

    @abstractmethod
    def _pre_update(self):
        pass

    def _update(self, **kwargs):
        params = []
        entries = []

        for fieldname, value in kwargs.items():
            setattr(self, fieldname, value)
            entries.append("{fieldname} = %s".format(fieldname=fieldname))
            params.append(value)

        for fieldname in self._additional_update_fields:
            entries.append("{fieldname} = %s".format(fieldname=fieldname))
            params.append(getattr(self, fieldname))

        params.append(self.id)

        update_sql = """
        UPDATE {table_name} SET
            {entries}
        WHERE id = %s""".format(table_name=self._table.fullname,
                                entries=",\n            ".join(entries))

        self._db.execute(update_sql, params, log_sql=self.argv.verbose)

    @_add_connection_resiliency
    def delete(self):
        if self._db is None:
            return

        self._delete()

    def _delete(self):
        delete_sql = """
        DELETE
        FROM {table_name}
        WHERE id = %s""".format(table_name=self._table.fullname)
        self._db.execute(delete_sql, [self.id], log_sql=self.argv.verbose)

    def _connect(self):
        if self._db.closed():
            self._db.connect(self._audit_conn_details)

    def __str__(self):
        return dump(self)


class ProcessControlBase(AuditBase):

    def __init__(self, table_name, mode, argv):
        super(ProcessControlBase, self).__init__(table_name, argv)
        self._additional_update_fields = [
            'process_starttime',
            'process_endtime',
            'duration'
        ]

        self.process_code = mode
        self._fieldnames.append('process_code')

        self.duration = 0
        self._fieldnames.append('duration')

        self.comment = const.EMPTY_STRING
        self._fieldnames.append('comment')

        self.status = const.IN_PROGRESS
        self._fieldnames.append('status')

        self.process_starttime = datetime.datetime.now()
        self._fieldnames.append('process_starttime')

        self.process_endtime = datetime.datetime.now()
        self._fieldnames.append('process_endtime')

        self.infolog = const.EMPTY_STRING
        self._fieldnames.append('infolog')

        self.errorlog = const.EMPTY_STRING
        self._fieldnames.append('errorlog')

    def _pre_insert(self):
        self._truncate_comment()

    def _pre_update(self):
        self._truncate_comment()
        self.process_endtime = datetime.datetime.now()
        timediff = self.process_endtime - self.process_starttime
        self.duration = timediff.total_seconds()

    def _truncate_comment(self):
        self.comment = self.comment[:const.MAX_COMMENT_LENGTH]


class ProcessControl(ProcessControlBase):

    def __init__(self, argv, mode):
        super(ProcessControl, self).__init__(const.PROCESS_CONTROL_TABLE,
                                             mode, argv)
        self.profile_name = self.argv.profilename
        self._fieldnames.append('profile_name')

        self.profile_version = self.argv.profileversion
        self._fieldnames.append('profile_version')

        self.process_name = mode
        self._fieldnames.append('process_name')

        self.min_lsn = None
        self._fieldnames.append('min_lsn')

        self.max_lsn = None
        self._fieldnames.append('max_lsn')

        self.filename = "{}".format(self.argv.outputfile)
        self._fieldnames.append('filename')

        self.executor_run_id = 0
        self._fieldnames.append('executor_run_id')

        self.executor_status = const.EMPTY_STRING
        self._fieldnames.append('executor_status')

        self.source_system_code = self.argv.profilename
        self._fieldnames.append('source_system_code')

        self.source_system_type = self.argv.sourcedbtype
        self._fieldnames.append('source_system_type')

        self.source_region = self.argv.sourceschema
        self._fieldnames.append('source_region')

        self.target_system = self.argv.profilename
        self._fieldnames.append('target_system')

        self.target_region = self.argv.targetschema
        self._fieldnames.append('target_region')

        self.target_system_type = self.argv.targetdbtype
        self._fieldnames.append('target_system_type')

        self.object_list = const.EMPTY_STRING
        self._fieldnames.append('object_list')

        self.total_count = 0
        self._fieldnames.append('total_count')


class ProcessControlDetail(ProcessControlBase):

    def __init__(self, argv, mode, parentid):
        super(ProcessControlDetail, self).__init__(
            const.PROCESS_CONTROL_DETAIL_TABLE, mode, argv)

        self.run_id = parentid
        self._fieldnames.append('run_id')

        self.object_schema = const.EMPTY_STRING
        self._fieldnames.append('object_schema')

        self.object_name = const.EMPTY_STRING
        self._fieldnames.append('object_name')

        self.source_row_count = 0
        self._fieldnames.append('source_row_count')

        self.insert_row_count = 0
        self._fieldnames.append('insert_row_count')

        self.update_row_count = 0
        self._fieldnames.append('update_row_count')

        self.delete_row_count = 0
        self._fieldnames.append('delete_row_count')

        self.bad_row_count = 0
        self._fieldnames.append('bad_row_count')

        self.alter_count = 0
        self._fieldnames.append('alter_count')

        self.create_count = 0
        self._fieldnames.append('create_count')

        self.delta_starttime = datetime.datetime.now()
        self._fieldnames.append('delta_starttime')

        self.delta_endtime = datetime.datetime.now()
        self._fieldnames.append('delta_endtime')

        self.delta_startlsn = const.EMPTY_STRING
        self._fieldnames.append('delta_startlsn')

        self.delta_endlsn = const.EMPTY_STRING
        self._fieldnames.append('delta_endlsn')

        self.error_message = const.EMPTY_STRING
        self._fieldnames.append('error_message')

        self.query_condition = const.EMPTY_STRING
        self._fieldnames.append('query_condition')


class SourceSystemProfile(AuditBase):

    def __init__(self, argv):
        super(SourceSystemProfile, self).__init__(
            const.SOURCE_SYSTEM_PROFILE_TABLE, argv)

        self.profile_name = self.argv.profilename
        self._fieldnames.append('profile_name')

        self.version = self.argv.profileversion
        self._fieldnames.append('version')

        self.source_system_code = self.argv.profilename
        self._fieldnames.append('source_system_code')

        self.source_region = self.argv.sourceschema
        self._fieldnames.append('source_region')

        self.target_region = self.argv.targetschema
        self._fieldnames.append('target_region')

        self.object_name = const.EMPTY_STRING
        self._fieldnames.append('object_name')

        self.object_seq = 0
        self._fieldnames.append('object_seq')

        self.min_lsn = None
        self._fieldnames.append('min_lsn')

        self.max_lsn = None
        self._fieldnames.append('max_lsn')

        self.active_ind = const.EMPTY_STRING
        self._fieldnames.append('active_ind')

        self.history_ind = const.EMPTY_STRING
        self._fieldnames.append('history_ind')

        self.applied_ind = const.EMPTY_STRING
        self._fieldnames.append('applied_ind')

        self.delta_ind = const.EMPTY_STRING
        self._fieldnames.append('delta_ind')

        self.last_run_id = 0
        self._fieldnames.append('last_run_id')

        self.last_process_code = const.EMPTY_STRING
        self._fieldnames.append('last_process_code')

        self.last_status = const.EMPTY_STRING
        self._fieldnames.append('last_status')

        self.last_updated = datetime.datetime.now()
        self._fieldnames.append('last_updated')

        self.last_applied = None
        self._fieldnames.append('last_applied')

        self.last_history_update = None
        self._fieldnames.append('last_history_update')

        self.notes = const.EMPTY_STRING
        self._fieldnames.append('notes')

    def __dir__(self):
        return self._fieldnames

    def _pre_insert(self):
        pass

    def _pre_update(self):
        self.last_updated = datetime.datetime.now()
