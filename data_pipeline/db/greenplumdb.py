###############################################################################
# Module:    greenplumdb
# Purpose:   Wraps the native Postgres client, exposing required operations on
#            Greenplum databases
#
# Notes:
#
###############################################################################

import data_pipeline.constants.const as const

from .postgresdb import PostgresDb


class GreenplumDb(PostgresDb):
    def __init__(self):
        super(GreenplumDb, self).__init__()

    @property
    def dbtype(self):
        return const.GREENPLUM
