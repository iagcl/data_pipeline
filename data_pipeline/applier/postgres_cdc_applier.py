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
# Module:    postgres_applier
# Purpose:   Applies CDCs polled from stream to a Postgres DB
#
# Notes:     This module focuses on logic for applying CDCs, often involving
#            execution of individual SQL statements
#
###############################################################################

import logging
import data_pipeline.constants.const as const
import data_pipeline.sql.utils as sql_utils

from .postgres_applier import PostgresApplier
from .exceptions import ApplyError


class PostgresCdcApplier(PostgresApplier):
    def __init__(self, target_db, argv, audit_factory, source_processor):
        super(PostgresCdcApplier, self).__init__(
            target_db, argv, audit_factory, source_processor)
        self._logger = logging.getLogger(__name__)

    def _build_update_sql(self, update_statement):
        return sql_utils.build_update_sql(update_statement,
                                          self._argv.targetschema)

    def _execute_statement(self, statement, commit_lsn):
        if self._can_buffer(statement):
            if self._bulk_ops.max_count == self._argv.bulkinsertlimit:
                self._logger.debug("Bulk apply insert limit hit: {c}. "
                                   "Executing bulk insert..."
                                   .format(c=self._bulk_ops.max_count))

                self._execute_bulk_ops()

            self._bulk_ops.add(statement,
                               commit_lsn,
                               self.current_message_offset)

            self.recovery_offset = self._bulk_ops.start_offset

            self._logger.debug(
                "Added statement {s}. Total count = {c}"
                .format(s=statement,
                        c=len(self._bulk_ops[statement.table_name])))
        else:
            # We've received a type other than insert, so we'll flush
            # all the accumulated insert statements out
            self._execute_bulk_ops()

            # Then execute the non-insert statement individually
            self.recovery_offset = self.current_message_offset
            sql = statement.tosql(self)

            if not sql:
                self._logger.warn(
                    "No resulting SQL string built from statement: "
                    "{statement}. Not executing..."
                    .format(statement=statement))
            else:
                self._execute_sql(sql, commit_lsn, self.current_message_offset)

    def _commit_statements(self):
        self._target_db.commit()
        self._logger.debug("Batch committed")

        self._output_file.write("{};\n".format(const.COMMIT))
        self._output_file.flush()
