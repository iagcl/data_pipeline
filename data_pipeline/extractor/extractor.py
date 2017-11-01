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
# Module:  extractor
# Purpose: Extracts records from a data source and publishes these records
#          to an output stream.
#
# Notes:
#
###############################################################################

import hashlib
import logging
import signal
import time
import traceback

import data_pipeline.audit.connection_factory as audit_conn_factory
import data_pipeline.constants.const as const
import data_pipeline.logger.logging_loader as logging_loader
import data_pipeline.utils.dbuser as dbuser
import data_pipeline.utils.mailer as mailer

from .exceptions import InvalidArgumentsError
from abc import ABCMeta, abstractmethod
from data_pipeline.common import SignalHandler
from data_pipeline.stream.factory import (build_kafka_producer,
                                          build_file_writer)


logger = logging.getLogger(__name__)


def existing_extract_is_running(audit_conn_details, argv):
    if argv.checkexistingextract:
        engine = audit_conn_factory.build_engine(audit_conn_details)
        with engine.connect() as conn:
            sqlstm = """
                SELECT status
                FROM {schema}.process_control
                WHERE 1 = 1
                  AND profile_name    = '{profilename}'
                  AND profile_version = {profileversion}
                  AND process_code = '{proc_code}'
                ORDER BY id DESC LIMIT 1;""".format(
                schema=argv.auditschema,
                profilename=argv.profilename,
                profileversion=argv.profileversion,
                proc_code=const.CDCEXTRACT)

            logger.debug("Executing: {}".format(sqlstm))
            result = conn.execute(sqlstm)

            for row in result:
                return row[0] == const.IN_PROGRESS

        return False


def get_schemas_and_tables(audit_conn_details, argv):
    engine = audit_conn_factory.build_engine(audit_conn_details)
    with engine.connect() as conn:
        sqlstm = """
            SELECT profile_name, source_region, object_name, active_ind
            FROM {}.source_system_profile
            WHERE 1 = 1
              AND profile_name = '{}'
              AND version      = {}
              AND last_status  = 'SUCCESS'
            ORDER BY object_seq""".format(
            argv.auditschema,
            argv.profilename,
            argv.profileversion)

        logger.debug("Executing: {}".format(sqlstm))
        result = conn.execute(sqlstm)
        active_schemas = set()
        active_tables = set()
        all_tables = set()

        def get_schema(row):
            return row[1]

        def get_table(row):
            return row[2]

        def is_active(row):
            return row[3] is not None and row[3] == 'Y'

        for row in result:
            if get_schema(row) and is_active(row):
                active_schemas.add(get_schema(row))

            if get_table(row):
                all_tables.add(get_table(row))
                if is_active(row):
                    active_tables.add(get_table(row))

        return (active_schemas, active_tables, all_tables)


class Extractor(SignalHandler):
    __metaclass__ = ABCMeta

    def __init__(self, mode, source_db, argv, audit_factory):
        super(Extractor, self).__init__(mode, argv, audit_factory)
        self._source_db = source_db
        self._keyfieldslist = {}
        self._kafka_producer = None
        self._batch_id = const.EMPTY_STRING
        self._output_file = None
        self._raw_output_file = None
        self._active_schemas = None
        self._active_tables = None
        self._all_tables = None
        self._sob_message_written = False
        self._last_written_lsn = None
        self._records_written_to_stream = 0
        self._start_lsn = None
        self._end_lsn = None

        self._pcd = audit_factory.build_process_control_detail(self._pc)

        self._init_output_file()
        self._init_raw_output_file()
        self._init_stream_output()

    def extract(self):
        if existing_extract_is_running(self._audit_conn_details, self._argv):
            self._logger.warn(
                "A earlier extract is currently running. Aborting extract...")
            return

        self._insert_pc()
        if self._write_terminate():
            return

        self._ensure_profile_is_set()
        self._ensure_schemas_and_tables_are_set()

        try:
            self.connect_to_source_db()
            self._extract_source_data()
            self._pc.status = const.SUCCESS
        except Exception, err:
            self._report_error(err)
        finally:
            self._write_eob_message()
            self.flush()

    @abstractmethod
    def _extract_source_data(self):
        pass

    def _init_output_file(self):
        if self._argv.outputfile is not None:
            self._output_file = build_file_writer(str(self._argv.outputfile))
            self._logger.info("Opened output file for writing: {}"
                              .format(self._argv.outputfile))

    def _init_raw_output_file(self):
        if self._argv.rawfile is not None:
            self._raw_output_file = build_file_writer(str(self._argv.rawfile))
            self._logger.info("Opened raw output file for writing: {}"
                              .format(self._argv.rawfile))

    def _init_stream_output(self):
        if self._kafka_streaming_enabled():
            self._kafka_producer = build_kafka_producer(self._argv)

    def _kafka_streaming_enabled(self):
        return (self._argv.streamhost and
                self._argv.streamchannel and
                self._argv.streamschemahost and
                self._argv.streamschemafile)

    def _ensure_profile_is_set(self):
        if not self._argv.profilename:
            raise InvalidArgumentsError("No profile name supplied. Extractor "
                                        "does not support extraction of all "
                                        "schemas and tables in a single run.")

        self._logger.info("Extracting profile: '{}'"
                          .format(self._argv.profilename))

    def _ensure_schemas_and_tables_are_set(self):
        (self._active_schemas,
         self._active_tables,
         self._all_tables) = get_schemas_and_tables(
            self._audit_conn_details, self._argv)

        if not (self._active_schemas and self._active_tables):
            raise InvalidArgumentsError(
                "Both schemas ({schemas}) and tables ({tables}) must be "
                "defined in source_system_profile for profile '{profile}'"
                .format(schemas=list(self._active_schemas),
                        tables=list(self._active_tables),
                        profile=self._argv.profilename))

        self._pc.object_list = ",".join(self._active_tables)
        self._logger.debug("Source system schemas = {}, tables = {}"
                           .format(self._active_schemas, self._active_tables))

    def _insert_pc(self):
        if logging_loader.logfiles:
            self._pc.infolog = logging_loader.get_logfile(const.INFO_HANDLER)
            self._pc.errorlog = logging_loader.get_logfile(const.ERROR_HANDLER)

        self._pc.comment = "Starting {mode} run".format(mode=self._mode)
        self._pc.status = const.IN_PROGRESS
        self._pc.insert()

    def _report_error(self, error):
        try:
            err_message = ("Failed {mode}: {error}"
                           .format(mode=self._mode, error=str(error)))
            self._pc.comment = err_message
            self._pc.status = const.ERROR

            if self._last_written_lsn:
                self._pc.max_lsn = self._last_written_lsn
                self._pc.status = const.WARNING
                self._end_lsn = self._pc.max_lsn

            self._pc.update()
            self._logger.exception(err_message)

            mailer.send(self._argv.notifysender,
                        self._argv.notifyerrorlist,
                        err_message,
                        self._argv.notifysmtpserver,
                        plain_text_message=err_message)
        except Exception, e:
            mail_send_err = ("Failed to send mail alert: {err}"
                             .format(err=str(e)))
            err_message = ("{err_message}\n{mail_send_err}"
                           .format(err_message=err_message,
                                   mail_send_err=mail_send_err))
            self._logger.exception(err_message)

    def _init_batch_id(self):
        self._batch_id = hashlib.md5("%.9f" % time.time()).hexdigest()

    def _reset_message(self):
        self._message.reset()
        self._message.batch_id = self._batch_id

    def _build_message(self, record_type, record_count=0,
                       commit_lsn=const.EMPTY_STRING):
        self._reset_message()
        self._message.record_type = record_type
        self._message.record_count = record_count
        self._message.commit_lsn = str(commit_lsn)
        return self._message

    def _write_sob_message(self, record_count):
        sob_message = self._build_message(
            const.START_OF_BATCH, record_count, self._start_lsn)
        self.write(sob_message.serialise())
        self._sob_message_written = True

    def _write_eob_message(self):
        if self._sob_message_written:
            eob_message = self._build_message(
                const.END_OF_BATCH,
                self._records_written_to_stream,
                self._end_lsn)

            self.write(eob_message.serialise())
            self._insert_pcd(const.EXTRACTOBJECTNAME,
                             self._records_written_to_stream)

    def _write_terminate(self):
        if self._argv.kill:
            terminate_message = self._build_message(const.KILL)
            self.write(terminate_message.serialise())
            self._pc.status = const.KILLED
            self.flush()
            return True

        return False

    def _insert_pcd(self, object_name, count):
        if logging_loader.logfiles:
            infolog = logging_loader.get_logfile(const.INFO_HANDLER)
            self._pcd.infolog = infolog

            errorlog = logging_loader.get_logfile(const.ERROR_HANDLER)
            self._pcd.errorlog = errorlog

        self._pcd.object_name = object_name
        self._pcd.process_code = self._mode
        self._pcd.source_row_count = count
        self._pcd.status = const.SUCCESS
        self._pcd.object_schema = self._argv.sourceschema
        self._pcd.comment = "Completed writing {mode}".format(mode=self._mode)
        self._pcd.insert()

    def write(self, message):
        self._logger.debug("Writing message to configured outputs: '{}'"
                           .format(str(message)))

        self.write_to_file(message)

        if not self._argv.donotload and not self._argv.donotsend:
            self.write_to_stream(message)

        self._last_written_lsn = message.get("commit_lsn")

    def write_to_file(self, message):
        if self._output_file is not None:
            self._output_file.write(str(message))
            self._output_file.write('\n')

    def write_to_rawfile(self, message):
        if self._raw_output_file is not None:
            self._raw_output_file.write(str(message))
            self._raw_output_file.write('\n')

    def write_to_stream(self, message):
        if self._kafka_producer is not None:
            t = type(message).__name__
            if t != const.DICT:
                raise TypeError("Message is not a dict type. Message type "
                                "passed: {}".format(t))

            if not self._kafka_producer.write(message):
                raise Exception("Failed to write to kafka stream")

    def disconnect_process_control(self):
        pass

    def connect_to_source_db(self):
        self._logger.info("Extractor connecting to db")
        conn_details = dbuser.get_dbuser_properties(self._argv.sourceuser)
        self._source_db.connect(conn_details)

    def _decorate_flush(func):
        def func_wrapper(self):
            self._pc.comment = "Sending records to stream ..."
            self._pc.update()

            func(self)

            self._pc.comment = ("Extracted {} transactions from source"
                                .format(self._records_written_to_stream))
            self._pc.total_count = self._records_written_to_stream
            self._pc.update()

        return func_wrapper

    @_decorate_flush
    def flush(self):
        if self._kafka_producer:
            self._kafka_producer.flush()
        if self._output_file:
            self._output_file.flush()
        if self._raw_output_file:
            self._raw_output_file.flush()

    def __del__(self):
        if self._output_file:
            self._logger.info("Closing output file: {}"
                              .format(self._argv.outputfile))
            self._output_file.close()
        if self._raw_output_file:
            self._logger.info("Closing raw output file: {}"
                              .format(self._argv.rawfile))
            self._raw_output_file.close()
