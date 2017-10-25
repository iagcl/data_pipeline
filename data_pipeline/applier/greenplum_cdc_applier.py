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
