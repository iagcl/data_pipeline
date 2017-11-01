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
# Module:    applier
# Purpose:   Applies CDCs polled from Kafka queue to a target DB
#
# Notes:
#
###############################################################################

import confluent_kafka
import logging
import os
import sys
import time
import yaml

import data_pipeline.audit.connection_factory as audit_conn_factory
import data_pipeline.constants.const as const
import data_pipeline.logger.logging_loader as logging_loader
import data_pipeline.sql.utils as sql_utils
import data_pipeline.utils.dbuser as dbuser
import data_pipeline.utils.filesystem as filesystem_utils
import data_pipeline.utils.mailer as mailer

from .exceptions import ApplyError
from abc import ABCMeta, abstractmethod
from data_pipeline.audit.audit_dao import SourceSystemProfile
from data_pipeline.audit.factory import AuditFactory, get_audit_db
from data_pipeline.common import SignalHandler
from data_pipeline.stream.file_writer import FileWriter
from data_pipeline.processor.exceptions import UnsupportedSqlError
from data_pipeline.utils.args import get_program_args


LSN = "lsn"
SQL = "sql"
OFFSET = "offset"


def _is_start_of_batch(message):
    return message.record_type == const.START_OF_BATCH


def _is_end_of_batch(message):
    return message.record_type == const.END_OF_BATCH


def _is_kill(message):
    return message.record_type == const.KILL


def get_inactive_applied_tables(audit_conn_details, argv, logger):
    sql = """
    SELECT profile_name, target_region, object_name
    FROM {audit_schema}.source_system_profile
    WHERE 1 = 1
      AND   profile_name = %s
      AND   version      = %s
      AND   COALESCE(applied_ind, 'Y') = 'N'
    ORDER BY object_seq""".format(audit_schema=argv.auditschema)
    bind_values = (argv.profilename, argv.profileversion)

    with get_audit_db(argv) as audit_db:
        logger.debug("Executing: {sql}\nBind values = {bind_values}"
                     .format(sql=sql, bind_values=bind_values))
        result = audit_db.execute_query(sql, argv.arraysize, bind_values)
        tables = set()

        for row in result:
            schema = const.EMPTY_STRING
            if row[1]:
                schema = "{schema}.".format(schema=row[1])
            if row[2]:
                tables.add("{schema}{table}"
                           .format(schema=schema, table=row[2])
                           .lower())

        logger.debug("Following tables will not be applied: {tables}"
                     .format(tables=tables))

        return tables


class CdcApplyRecord:
    def __init__(self, executor_run_id, executor_status, status):
        self.executor_run_id = executor_run_id
        self.executor_status = executor_status
        self.status = status


class BulkOperation(object):
    def __init__(self):
        self._buff = {}
        self.reset()

    def keys(self):
        return self._buff.keys()

    def items(self):
        return self._buff.items()

    def empty(self):
        return self.max_count == 0

    def __getitem__(self, key):
        return self._buff[key]

    def reset(self):
        self.max_count = 0
        self.max_lsn = 0
        self.max_offset = 0
        self.start_offset = 0
        self.statement_type = None

    def add(self, statement, commit_lsn, offset):
        if (self.statement_type and
                self.statement_type != statement.statement_type):
            raise Exception("Attempting to add statement with operation: "
                            "{statement_op} but current bulk operation is for "
                            "opertion: {bulk_op}"
                            .format(statement_op=statement.statement_type,
                                    bulk_op=self.statement_type))

        self.statement_type = statement.statement_type

        if self.empty():
            self.start_offset = offset

        statements = self._buff.setdefault(statement.table_name, [])
        statements.append((statement, commit_lsn, offset))

        self.max_count = max(self.max_count, len(statements))
        self.max_lsn = max(self.max_lsn, commit_lsn)
        self.max_offset = max(self.max_offset, offset)

    def __str__(self):
        return str(self._buff)


class Applier(SignalHandler):
    __metaclass__ = ABCMeta

    def __init__(self, mode, target_db, argv, audit_factory, source_processor):
        super(Applier, self).__init__(mode, argv, audit_factory)
        self._target_db = target_db
        self._source_processor = source_processor
        self._output_file = None
        self._batch_started = False
        self._target_conn_details = dbuser.get_dbuser_properties(
            argv.targetuser)
        self._target_conn_details.sslmode = self._argv.sslmode
        self._target_conn_details.sslcert = self._argv.sslcert
        self._target_conn_details.sslrootcert = self._argv.sslrootcert
        self._target_conn_details.sslkey = self._argv.sslkey
        self._target_conn_details.sslcrl = self._argv.sslcrl

        self._init()
        self._init_auditing()
        self._init_output_file()

        self._maxlsns_per_table = {}
        self._delta_maxlsns_per_table = {}
        self._get_max_lsn_source_system_profile()

        self._last_apply_record = None
        self._get_last_apply_record()

        self._first_batch_received = False
        self._skip_batches = int(self._argv.skipbatch)

        stream = file(argv.datatypemap)
        self._config = yaml.load(stream)
        self._stream_message = None

        self._bulk_ops = BulkOperation()

        self._committed_state = {}
        self._last_executed_state = {}
        self._last_committed_state = None

    @property
    def recovery_offset(self):
        if not self._recovery_offset:
            return self.current_message_offset

        return self._recovery_offset

    @recovery_offset.setter
    def recovery_offset(self, value):
        self._logger.debug("Setting recovery offset = {}".format(value))
        self._recovery_offset = value

    @property
    def at_auditcommitpoint(self):
        return (self._received_count > 0 and
                self._received_count % self._argv.auditcommitpoint == 0)

    @property
    def at_targetcommitpoint(self):
        return (self._received_count > 0 and
                self._received_count % self._argv.targetcommitpoint == 0)

    @property
    def next_offset_to_read(self):
        # No record previous apply records found
        if self._last_apply_record is None:
            return None
        return self._last_apply_record.executor_run_id

    @property
    def last_status(self):
        # No record previous apply records found
        if self._last_apply_record is None:
            return None
        return self._last_apply_record.status

    @property
    def current_message_offset(self):
        return self._stream_message.offset()

    @property
    def next_message_offset(self):
        return self._stream_message.offset() + 1

    def apply(self, stream_message):
        self._stream_message = stream_message
        message = self._source_processor.deserialise(stream_message.value())
        batch_committed = False
        retries_remaining = self._argv.retry

        while retries_remaining >= 0:
            try:
                if self._can_apply(message):
                    if _is_kill(message):
                        self._log_terminate()
                        return const.KILLED
                    elif _is_start_of_batch(message):
                        self._start_batch(message)
                    elif _is_end_of_batch(message):
                        batch_committed = self._end_batch(message)
                    else:
                        self._apply_data(message)

                if not batch_committed:
                    self._audit_commit()
                    self._target_commit(message)

                break

            except Exception, e:
                err_message = "{err}\n".format(err=str(e))
                self.report_error(err_message)

                if retries_remaining == 0:
                    return const.ERROR

                time.sleep(self._argv.retrypause)
                self._logger.info("Retrying apply... remaining retries = {r}"
                                  .format(r=retries_remaining))
                retries_remaining -= 1

        # Update the next offset to read in case a reassignment is triggered
        self._last_apply_record.executor_run_id = self.next_message_offset

        return const.COMMITTED if batch_committed else const.UNCOMMITTED

    def _log_terminate(self):
        self._init_auditing()
        warn_message = ("Termination message received. "
                        "Shutting down consumer.")
        self._pc.comment = warn_message
        self._pc.status = const.KILLED
        self._pc.executor_run_id = self.next_message_offset
        self._pc.executor_status = const.COMMITTED
        self._pc.update()
        self._logger.warn(warn_message)

    def _can_apply(self, message):
        t = sql_utils.TableName(self._argv.targetschema.lower(),
                                message.table_name.lower())

        if t.fullname in self._inactive_applied_tables:
            self._logger.warn("Table {t} marked as inactive for applies. "
                              "Message will not be applied."
                              .format(t=t.fullname))
            return False

        if t.fullname in self._maxlsns_per_table:
            if not message.commit_lsn:
                self._logger.warn("[{t}] Message LSN is not set for message. "
                                  "Allowing message to be applied "
                                  "to target: {message}"
                                  .format(t=t.fullname,
                                          message=str(message)))
                return True

            if not self._maxlsns_per_table[t.fullname]:
                self._logger.warn("[{t}] Max LSN is not set in "
                                  "source_system_profile table. Allowing "
                                  "message to be applied to target: {message}"
                                  .format(t=t.fullname,
                                          message=str(message)))
                return True

            message_lsn = int(message.commit_lsn)
            max_lsn = int(self._maxlsns_per_table[t.fullname])

            self._logger.debug("[{t}] Making sure message LSN ({msglsn}) > "
                               "Max recorded LSN ({maxlsn})"
                               .format(t=t.fullname,
                                       msglsn=message_lsn,
                                       maxlsn=max_lsn))

            if message_lsn <= max_lsn:
                self._logger.warn("[{t}] Message LSN ({msglsn}) <= Max "
                                  "recorded LSN ({maxlsn}). "
                                  "Message will not be applied."
                                  .format(msglsn=message_lsn,
                                          maxlsn=max_lsn,
                                          t=t.fullname))
                return False

        return True

    def _get_last_apply_record(self):
        if self._last_apply_record is None:
            self._last_apply_record = CdcApplyRecord(
                executor_run_id=None,
                executor_status=const.SUCCESS,
                status=const.SUCCESS
            )

        if self._argv.seektoend:
            self._last_apply_record.executor_run_id = confluent_kafka.OFFSET_END
            self._last_apply_record.executor_status = const.SUCCESS
            self._last_apply_record.status = const.SUCCESS
            return

        sql = """
        SELECT executor_run_id, executor_status, status
        FROM {audit_schema}.process_control
        WHERE id = (
                    SELECT MAX(id)
                    FROM process_control
                    WHERE executor_run_id > 0
                      AND profile_name    = %s
                      AND profile_version = %s
                      AND process_code    = %s
                   )
        """.format(audit_schema=self._argv.auditschema,
                   committed=const.COMMITTED)
        bind_variables = (self._argv.profilename,
                          self._argv.profileversion,
                          self._mode)

        with get_audit_db(self._argv) as audit_db:
            query_results = audit_db.execute_query(
                sql, self._argv.arraysize, bind_variables)

            row = query_results.fetchone()
            if row:
                self._last_apply_record.executor_run_id = row[0]
                self._last_apply_record.executor_status = row[1]
                self._last_apply_record.status = row[2]
                self._logger.info("Last committed offset = {offset}"
                                  .format(offset=self.next_offset_to_read))

    def _get_max_lsn_source_system_profile(self):
        sql = """
        SELECT target_region, object_name, max_lsn
        FROM {audit_schema}.source_system_profile
        WHERE profile_name    = %s
          AND version = %s
        """.format(audit_schema=self._argv.auditschema)
        bind_variables = (self._argv.profilename,
                          self._argv.profileversion)

        with get_audit_db(self._argv) as audit_db:
            query_results = audit_db.execute_query(
                sql, self._argv.arraysize, bind_variables)

            for row in query_results:
                target_region = row[0].lower()
                table_name = row[1].lower()
                max_lsn = row[2]

                t = sql_utils.TableName(target_region, table_name)
                self._logger.debug("Mapping table->max_lsns from "
                                   "source_system_profile: {t}->{l}"
                                   .format(t=t.fullname, l=max_lsn))

                if max_lsn is not None:
                    self._maxlsns_per_table[t.fullname.lower()] = max_lsn

    def _apply_data(self, message):
        if self._skip_batches < 0:
            raise Exception("Invalid state: Skip batches < 0")
        elif self._skip_batches == 0:
            self._received_count += 1

            table = sql_utils.TableName(self._argv.targetschema,
                                        message.table_name)
            tablename = table.fullname.lower()
            self._delta_maxlsns_per_table[tablename] = message.commit_lsn

            if not self._batch_started:
                # Insert an implicit start of batch
                self._start_batch(message)

            if message.table_name:
                pcd = self.get_pcd(message.table_name)
                pcd.source_row_count += 1
            else:
                raise ApplyError("table_name has not been "
                                 "specified in message")

            try:
                statement = self._source_processor.process(message)
                if statement:
                    # Create an entry in source_system_profile if the
                    # table_name doesn't already exist
                    self._ensure_table_name_in_ssp(
                        statement.statement_type, message)

                    self.execute_statement(statement, message.commit_lsn)

            except UnsupportedSqlError, err:
                self._logger.warn("Unsupported SQL in {msg}: {error}"
                                  .format(msg=message, error=str(err)))
            self._processed_count += 1
        else:
            self._logger.debug("Skipping message...")

    def _ensure_table_name_in_ssp(self, statement_type, message):
        if statement_type != const.CREATE:
            return

        sql = ("""
        -- Insert a new source_system_profile record for object
        -- '{table_name}' if it doesn't already exist for the current profile
        INSERT INTO {schema}.source_system_profile
            (profile_name, version, source_system_code, target_region,
             object_name, min_lsn, max_lsn, active_ind,
             last_process_code, last_status,
             last_updated, last_applied, object_seq)
        SELECT
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
            ( -- Compute the next object_seq in the profile
                SELECT MAX(object_seq)+1 FROM {schema}.source_system_profile
                WHERE 1 = 1
                  AND profile_name = %s
                  AND version = %s
            )
        WHERE NOT EXISTS ( -- Make this operation idempotent
            SELECT * FROM {schema}.source_system_profile
            WHERE 1 = 1
              AND profile_name         = %s
              AND version              = %s
              AND LOWER(target_region) = LOWER(%s)
              AND LOWER(object_name)   = LOWER(%s)
        )""".format(schema=self._argv.auditschema,
                    table_name=message.table_name))

        with get_audit_db(self._argv) as audit_db:
            bind_values = (
                self._argv.profilename,
                self._argv.profileversion,
                self._argv.sourcesystem,
                self._argv.targetschema,
                message.table_name.lower(),
                message.commit_lsn,
                message.commit_lsn,
                'Y',
                const.CDCAPPLY,
                const.SUCCESS,

                self._argv.profilename,
                self._argv.profileversion,

                self._argv.profilename,
                self._argv.profileversion,
                self._argv.targetschema,
                message.table_name.lower(),
            )
            affected_rows = audit_db.execute(sql, bind_values)
            audit_db.commit()

    def report_error(self, err_message):
        try:
            self._pc.comment = err_message
            self._pc.status = const.ERROR
            self._pc.update()

            subject = ("{source_system} applier has failed. Partial batch "
                       "(up to error) committed."
                       .format(source_system=self._argv.profilename))

            # More detailed error message for email and logs
            current_message = None
            if self._stream_message:
                current_message = str(self._stream_message.value())

            err_message = (
                "Error: {original_message}\n"
                "***STATE DUMP***\n\n"
                "Last executed state: {last_executed_state}\n\n"
                "Last committed state: {last_committed_state}\n\n"
                "Current message: {current_message}\n\n"
                .format(original_message=err_message,
                        last_executed_state=str(self._last_executed_state),
                        last_committed_state=str(self._last_committed_state),
                        current_message=current_message))

            mailer.send(self._argv.notifysender,
                        self._argv.notifyerrorlist,
                        subject,
                        self._argv.notifysmtpserver,
                        plain_text_message=err_message)
        except Exception, e:
            mail_send_err = ("Failed to send mail alert: {err}"
                             .format(err=str(e)))
            err_message = ("{err_message}\n{mail_send_err}"
                           .format(err_message=err_message,
                                   mail_send_err=mail_send_err))

        self._logger.exception(err_message)

    def _init(self):
        self._total_count = 0
        self._received_count = 0
        self._executed_count = 0
        self._processed_count = 0
        self._pc = None
        self._pcds = {}
        self._recovery_offset = None

    def _init_output_file(self):
        self._output_file = FileWriter(self._argv.outputfile, 'a+')

    def _init_auditing(self, comment=const.EMPTY_STRING):
        self._inactive_applied_tables = get_inactive_applied_tables(
            self._audit_conn_details, self._argv, self._logger)

        self._pc = self._audit_factory.build_process_control(self._mode)
        self._pc.comment = comment

        if logging_loader.logfiles:
            self._pc.infolog = logging_loader.get_logfile(const.INFO_HANDLER)
            self._pc.errorlog = logging_loader.get_logfile(const.ERROR_HANDLER)

        self._pc.insert()
        self._pcds.clear()
        self._logger.debug("Initialised process control for Applier")

    @property
    def process_control(self):
        return self._pc

    @abstractmethod
    def _execute_statement(self, statement):
        """Execute statement on target
        :param Statement statement: The statement to execute on target
        """
        pass

    @abstractmethod
    def _commit_statements(self):
        """Commit all executed statements/transactions on target
        """
        pass

    def build_insert_sql(self, statement):
        pcd = self.get_pcd(statement.table_name)
        pcd.insert_row_count += 1
        self._logger.debug("Insert count for {t} = {c}"
                           .format(t=statement.table_name,
                                   c=pcd.insert_row_count))
        return self._build_insert_sql(statement)

    @abstractmethod
    def _build_insert_sql(self, statement):
        """Builds target-specific insert sql statement
        :param InsertStatement statement: The representation of an
            Insert statement containing all necessary details to
            create an Insert SQL statement
        """
        pass

    def build_update_sql(self, statement):
        pcd = self.get_pcd(statement.table_name)
        pcd.update_row_count += 1
        self._logger.debug("{t} update row count = {c}"
                           .format(t=statement.table_name,
                                   c=pcd.update_row_count))
        return self._build_update_sql(statement)

    @abstractmethod
    def _build_update_sql(self, statement):
        """Builds target-specific update sql statement
        :param UpdateStatement statement: The representation of an
            Update statement containing all necessary details to
            create an Update SQL statement
        """
        pass

    def build_delete_sql(self, statement):
        pcd = self.get_pcd(statement.table_name)
        pcd.delete_row_count += 1
        self._logger.debug("{t} delete row count = {c}"
                           .format(t=statement.table_name,
                                   c=pcd.delete_row_count))
        return self._build_delete_sql(statement)

    @abstractmethod
    def _build_delete_sql(self, statement):
        """Builds target-specific delete sql statement
        :param DeleteStatement statement: The representation of a
            Delete statement containing all necessary details to
            create an Delete SQL statement
        """
        pass

    def build_alter_sql(self, statement):
        pcd = self.get_pcd(statement.table_name)
        pcd.alter_count += 1
        return self._build_alter_sql(statement)

    @abstractmethod
    def _build_alter_sql(self, statement):
        """Builds target-specific alter sql statement
        :param AlterStatement statement: The representation of an
            Alter statement containing all necessary details to
            create an Alter SQL statement
        """
        pass

    def build_create_sql(self, statement):
        pcd = self.get_pcd(statement.table_name)
        pcd.create_count += 1

        self._append_metacols(statement)

        return self._build_create_sql(statement)

    def _append_metacols(self, statement):
        if not self._argv.metacols:
            return

        for name, fieldname in self._argv.metacols.items():
            statement.add_entry(
                field_name=fieldname,
                data_type='TIMESTAMP',
                params=None,
                constraints=None
            )

    @abstractmethod
    def _build_create_sql(self, statement):
        """Builds target-specific create sql statement
        :param CreateStatement statement: The representation of a
            Create statement containing all necessary details to
            create an Create SQL statement
        """
        pass

    @abstractmethod
    def get_target_datatype(self, datatype, params):
        """Gets the target datatype string given the source datatype
        and associated params
        :param string datatype: The source data type
        :param list params: The source list of associated params for
            the datatype
        """
        pass

    def get_pcd(self, table_name):
        pcd = self._pcds.get(table_name, None)

        if pcd is None:
            pcd = self._audit_factory.build_process_control_detail(self._pc)
            pcd.object_name = table_name
            pcd.insert()

            self._pcds[table_name] = pcd

        return pcd

    def _commit(self, message, comment=const.EMPTY_STRING,
                status=const.SUCCESS, commit_offset=None):
        self._pc.max_lsn = message.commit_lsn

        if commit_offset is None:
            commit_offset = self.next_message_offset
        if comment:
            comment += "\n"

        committed = False

        if self._argv.donotcommit:
            comment += "Do not commit set, aborting commit"
            status = const.WARNING
        elif self._skip_batches > 0:
            comment += "Skip batch requested. Offsets will be committed"
            status = const.SKIPPED
            committed = True
        else:
            self._commit_statements()
            comment += ("Committed {} transactions on target"
                        .format(self._processed_count))
            committed = True

            self._logger.debug("Saving offset: {o}".format(o=commit_offset))

            self._last_committed_state = self._last_executed_state.copy()

            self._pc.executor_run_id = commit_offset
            self._pc.executor_status = const.COMMITTED

        self._logger.info(comment)
        self._pc.comment = comment
        self._pc.status = status
        self._pc.update()

        for pcd in self._pcds.itervalues():
            pcd.status = status
            pcd.comment = comment
            pcd.update()

        return committed

    def _audit_commit(self):
        if self.at_auditcommitpoint:
            self._logger.debug("Audit commit point reached ({msgcount}). "
                               "Committing..."
                               .format(msgcount=self._received_count))
            self._pc.update()
            for pcd in self._pcds.itervalues():
                pcd.update()

    def _target_commit(self, message):
        if self.at_targetcommitpoint:
            self._logger.info("Target commit point reached ({c}). "
                              "Committing...".format(c=self._received_count))

            self._commit(message, status=const.IN_PROGRESS)

    def _check_record_counts(self, end_of_batch_count, name_count_pairs):
        for (name, actual_count) in name_count_pairs:
            if end_of_batch_count != actual_count:
                self._logger.warn(
                    "End of batch record count ({end_of_batch_count}) "
                    "!= {count_name} record count ({actual_count})"
                    .format(end_of_batch_count=end_of_batch_count,
                            count_name=name,
                            actual_count=actual_count))

    def _end_batch(self, message):
        # Empty out the buffer of statements
        self._execute_bulk_ops()

        committed = self._commit(message)

        if self._skip_batches > 0:
            self._skip_batches -= 1
            self._logger.info("Skipped batch: {id}. {cnt} more batches to "
                              "skip..."
                              .format(id=message.batch_id,
                                      cnt=self._skip_batches))
        else:
            name_count_pairs = [("received", self._received_count),
                                ("processed", self._processed_count),
                                ("executed", self._executed_count)]

            self._check_record_counts(message.record_count, name_count_pairs)
            if not self._batch_started:
                raise ApplyError("END_OF_BATCH received while already ended")

        self._batch_started = False

        self._logger.info("Disconnecting from target")
        self._target_db.close()

        self._update_ssp_max_lsn()

        assert self._target_db.closed()

        return committed

    def _update_ssp_max_lsn(self):
        sql = ("""
        UPDATE {schema}.source_system_profile
        SET last_process_code = %s, max_lsn = %s
        WHERE 1 = 1
          AND profile_name         = %s
          AND version              = %s
          AND LOWER(target_region) = LOWER(%s)
          AND LOWER(object_name)   = LOWER(%s)""".format(
              schema=self._argv.auditschema))

        with get_audit_db(self._argv) as audit_db:
            items = self._delta_maxlsns_per_table.items()
            for full_table_name, max_lsn in items:
                (schema, table_name) = full_table_name.split('.')
                bind_values = (self._mode,
                               max_lsn,
                               self._argv.profilename,
                               self._argv.profileversion,
                               schema,
                               table_name)

                affected_rows = audit_db.execute(sql, bind_values)

            audit_db.commit()

        self._maxlsns_per_table = dict(self._maxlsns_per_table,
                                       **self._delta_maxlsns_per_table)
        self._delta_maxlsns_per_table.clear()

    def renew_workdirectory(self):
        self._argv = get_program_args(self._mode)
        logging_loader.setup_logging(self._argv.workdirectory)
        self._init_output_file()
        self._logger = logging.getLogger(__name__)
        self._logger.debug("New workdirectory set to: {workdir}"
                           .format(workdir=self._argv.workdirectory))
        self._source_processor.renew_workdirectory()
        self._target_db.renew_workdirectory()

    def _start_batch(self, message):
        self._logger.info("Connecting to target")
        assert self._target_db.closed()
        self._target_db.connect(self._target_conn_details)
        assert not self._target_db.closed()

        if self._first_batch_received:
            self.renew_workdirectory()

            self._init()
            self._init_auditing()
            self._get_last_apply_record()
        else:
            self._first_batch_received = True

        if self._batch_started:
            raise ApplyError("START_OF_BATCH received while already started")

        self._pc.min_lsn = message.commit_lsn
        self._pc.total_count = message.record_count
        self._batch_started = True
        self._logger.info("Batch started. Waiting for DATA records...")
        if self._skip_batches > 0:
            self._logger.warn("Warning: This batch will be skipped...")

    def connect_data_target(self, conn_details):
        """Connect to the target data store
        :param ConnectionDetails conn_details: The object containing
            all details necessary for connecting to the target data store
        """
        self._target_db.connect(conn_details)

    def execute_statement(self, statement, commit_lsn):
        if self._argv.donotapply:
            self._logger.debug("Do not apply set, aborting execution")
        else:
            self._execute_statement(statement, commit_lsn)
            self._executed_count += 1
            self._pc.comment = ("Committed {c} transactions to target"
                                .format(c=self._executed_count))

    def _execute_bulk_ops(self):
        """Execute statements for all tables."""
        if not self._bulk_ops.empty():
            self.recovery_offset = self._bulk_ops.start_offset
            self._logger.debug("Executing bulk {op} for tables: {tables}"
                               .format(op=self._bulk_ops.statement_type,
                                       tables=self._bulk_ops.keys()))

            for table_name, statements in self._bulk_ops.items():
                self._execute_bulk_op(self._bulk_ops.statement_type,
                                      table_name, statements)
                del statements[:]

            self._logger.debug("Table inserts cache should be empty: {cache}"
                               .format(cache=self._bulk_ops))

            self._bulk_ops.reset()
            self.recovery_offset = None

    def _execute_bulk_op(self, statement_type, table_name, statements):
        if statements:
            sql = None
            pcd = self.get_pcd(table_name)
            if statement_type == const.INSERT:
                sql = sql_utils.build_bulk_insert_sql(
                    statements,
                    self._argv.targetschema
                )

                if sql:
                    self._execute_sql(sql,
                                      self._bulk_ops.max_lsn,
                                      self._bulk_ops.max_offset)

                    pcd.insert_row_count += len(statements)

                self._logger.debug("{t} insert row count = {c}"
                                   .format(t=table_name,
                                           c=pcd.insert_row_count))
            else:
                raise Exception("Unsupported bulk operation: {op}"
                                .format(op=statement_type))

    def _execute_sql(self, sql, commit_lsn=None, offset=None):
        if commit_lsn is None and offset is None:
            lsn_comment_str = const.EMPTY_STRING
        else:
            lsn_comment_str = ("; -- lsn: {lsn}, offset: {offset}"
                               .format(lsn=commit_lsn, offset=offset))

        sql = ("{sql}{lsn_comment_str}"
               .format(sql=sql, lsn_comment_str=lsn_comment_str))

        self._last_executed_state[SQL] = sql
        if commit_lsn:
            self._last_executed_state[LSN] = commit_lsn
        if offset:
            self._last_executed_state[OFFSET] = offset

        self._logger.debug("Executing: {}".format(sql))
        count = self._target_db.execute(sql)
        self._output_file.write("{}\n".format(sql))
        self._logger.debug("Successfully executed - {c}".format(c=count))

    def _can_buffer(self, statement):
        return (self._argv.bulkapply and
                statement.statement_type == const.INSERT)
