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
# Module:    greenplum_applier
# Purpose:   Applies CDCs polled from stream to a Greenplum DB
#
# Notes:     Greenplum is essentially a Postgres DB, hence why it inherits from
#            PostgresCdcApplier. The only differentiator is the need to remove
#            primary keys from the SET clause of an UPDATE statement.
#
###############################################################################

import logging
import data_pipeline.constants.const as const
import data_pipeline.sql.utils as sql_utils

from .postgres_cdc_applier import PostgresCdcApplier
from .exceptions import ApplyError


class GreenplumCdcApplier(PostgresCdcApplier):

    def _build_update_sql(self, update_statement):

        def is_not_a_primary_key(field_name):
            if update_statement.primary_key_list:
                return field_name not in update_statement.primary_key_list
            return True

        return sql_utils.build_update_sql(update_statement,
                                          schema=self._argv.targetschema,
                                          filter_func=is_not_a_primary_key)
