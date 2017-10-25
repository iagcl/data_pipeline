###############################################################################
# Module:    oracle_message
# Purpose:   Utility to define an oracle kafka message in python object and
#            provide methods to serialise and deserialise the message
#
# Notes:
#
###############################################################################

from .base_message import BaseMessage


class FileStreamMessage(BaseMessage):
    """Represents the contents of a line/record from a data file
    """
    def __init__(self):
        super(FileStreamMessage, self).__init__()

    def reset(self):
        super(FileStreamMessage, self).reset()
        self.payload = const.EMPTY_STRING
