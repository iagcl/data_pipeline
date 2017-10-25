###############################################################################
# Module:    greenplumdb
# Purpose:   Contains greenplum specific initsync functions. Given greenplum
#            and postgres essentially share the same API, this module should
#            just inherit all functions from postgresdb
#
# Notes:
#
###############################################################################


from postgresdb import PostgresDb


class GreenplumDb(PostgresDb):
    def __init__(self, argv, db, logger):
        super(PostgresDb, self).__init__(argv, db, logger)
