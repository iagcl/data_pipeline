###############################################################################
# Module:    exceptions
# Purpose:   Collection of custom exceptions for the processor package
#
# Notes:
#
###############################################################################


class UnsupportedSqlError(Exception):
    """Exceptions raised when unsupported SQL is read.
       :param str message: The error message
    """

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message
