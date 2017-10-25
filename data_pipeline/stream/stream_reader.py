###############################################################################
# Module:  stream_reader
# Purpose: Reads records in from a stream, sending these records to a recipient
#
# Notes:
#
###############################################################################

from abc import ABCMeta, abstractmethod


class StreamReader(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def read_to(self, recipient):
        """Read records from a stream and send to the given recipient
        :param object recipient: Recipient of the messages read from stream
        """
        pass
