###############################################################################
# Module:  factory
# Purpose: Build concrete instances of specific database clients
#
# Notes:
#
###############################################################################

from .exceptions import UnsupportedDbTypeError
import data_pipeline.constants.const as const


def build(dbtype_name):
    """Return the specific type of db object given the dbtype_name"""
    if dbtype_name == const.ORACLE:
        from .oracledb import OracleDb
        return OracleDb()
    elif dbtype_name == const.MSSQL:
        from .mssqldb import MssqlDb
        return MssqlDb()
    elif dbtype_name == const.POSTGRES:
        from .postgresdb import PostgresDb
        return PostgresDb()
    elif dbtype_name == const.GREENPLUM:
        from .greenplumdb import GreenplumDb
        return GreenplumDb()
    elif dbtype_name == const.FILE:
        from .filedb import FileDb
        return FileDb()
    else:
        raise UnsupportedDbTypeError(dbtype_name)
