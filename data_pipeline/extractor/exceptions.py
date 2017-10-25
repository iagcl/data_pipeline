###############################################################################
# Module:    exceptions
# Purpose:   Collection of custom exceptions for the extractor package
#
# Notes:
#
###############################################################################


class InvalidArgumentsError(Exception):
    """Thrown when arguments provided to a function are invalid."""

    def __init__(self, message):
        super(InvalidArgumentsError, self).__init__(message)
