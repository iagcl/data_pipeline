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
# Module:  cdc_extractor
# Purpose: Extracts CDC records from a data source
#
# Notes:
#
###############################################################################

import logging
import data_pipeline.audit.connection_factory as audit_conn_factory
import data_pipeline.constants.const as const

from .exceptions import InvalidArgumentsError
from .extractor import Extractor
from abc import ABCMeta, abstractmethod


logger = logging.getLogger(__name__)


def get_prev_run_cdcs(audit_conn_details, argv):
    engine = audit_conn_factory.build_engine(audit_conn_details)
    with engine.connect() as conn:
        sqlstm = """
            SELECT id
              ,process_code
              ,min_lsn
              ,max_lsn
              ,status
            FROM {schema}.process_control
            WHERE id = (
                SELECT MAX(id) FROM {schema}.process_control
                WHERE 1 = 1
                AND   profile_name    = '{profilename}'
                AND   profile_version = {profileversion}
                AND   process_code   IN ('{process1}','{process2}')
                AND   status         IN ('{status1}', '{status2}')
                AND   min_lsn        IS NOT NULL
            )""".format(schema=argv.auditschema,
                        profilename=argv.profilename,
                        profileversion=argv.profileversion,
                        process1=const.INITSYNC,
                        process2=const.CDCEXTRACT,
                        status1=const.SUCCESS,
                        status2=const.WARNING)

        logger.info("Executing: {}".format(sqlstm))

        result = conn.execute(sqlstm)
        for row in result:
            return (row[2], row[3])


class CdcExtractor(Extractor):
    __metaclass__ = ABCMeta

    def __init__(self, db, argv, audit_factory):
        super(CdcExtractor, self).__init__(
            const.CDCEXTRACT, db, argv, audit_factory)

    def _extract_source_data(self):
        self.build_keycolumnlist(self._active_schemas, self._active_tables)

        (self._start_lsn, self._end_lsn) = self._get_cdc_query_range()
        if self._end_lsn is not None:
            self.poll_cdcs(self._start_lsn, self._end_lsn)
            self.flush()
        else:
            self._report_no_cdcs_found()

        return self._end_lsn

    def _get_cdc_query_range(self):
        (prev_min_cdc_point, prev_max_cdc_point) = get_prev_run_cdcs(
            self._audit_conn_details, self._argv)

        self._pc.min_lsn = prev_min_cdc_point
        self._pc.max_lsn = prev_max_cdc_point

        if self._argv.startscn:
            self._logger.info(
                "Overriding computed minscn ({minscn}) with "
                "supplied startscn ({startscn})"
                .format(minscn=prev_max_cdc_point,
                        startscn=self._argv.startscn))

            prev_max_cdc_point = self._argv.startscn

        (min_cdc_point, max_cdc_point) = self.get_source_minmax_cdc_point(
            prev_max_cdc_point)

        if self._argv.endscn:
            self._logger.info(
                "Overriding computed maxscn ({maxscn}) with "
                "supplied endscn ({endscn})"
                .format(maxscn=max_cdc_point, endscn=self._argv.endscn))
            max_cdc_point = self._argv.endscn

        return (min_cdc_point, max_cdc_point)

    def _report_no_cdcs_found(self):
        message = "Completed CDCExtract - No new CDC Records detected ..."
        self._pc.comment = message
        self._pc.status = const.SUCCESS
        self._pc.update()
        self._logger.debug(message)

    @abstractmethod
    def get_source_minmax_cdc_point(self, min_lsn):
        pass

    @abstractmethod
    def build_keycolumnlist(self):
        pass

    def _decorate_poll_cdcs(func):
        def func_wrapper(self, beg_cdc_point, end_cdc_point):
            self._pc.min_lsn = beg_cdc_point
            self._pc.max_lsn = end_cdc_point
            self._pc.comment = "Extracting CDC Records ..."
            self._pc.update()

            func(self, beg_cdc_point, end_cdc_point)

            self._pc.comment = "Extracted CDC Records."
            self._pc.update()

        return func_wrapper

    @_decorate_poll_cdcs
    def poll_cdcs(self, beg_cdc_point, end_cdc_point):
        pass
