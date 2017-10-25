###############################################################################
# Module:    exceptions
# Purpose:   Collection of custom exceptions for the db package
#
# Notes:
#
###############################################################################


class MssqlDatabaseException(Exception):
    pass


class UnsupportedDbTypeError(Exception):
    """Exceptions raised when an unsupported DbType is used."""

    def __init__(self, dbtype):
        self.dbtype = dbtype
