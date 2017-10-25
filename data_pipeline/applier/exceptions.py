###############################################################################
# Module:    exceptions
# Purpose:   Collection of custom exceptions for the applier package
#
# Notes:
#
###############################################################################


class ApplyError(Exception):
    """Exceptions raised when an unsupported DbType is used.

    Attributes:
        dbtype -- the DbType that was used
    """

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message
