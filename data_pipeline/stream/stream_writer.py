###############################################################################
# Module:  stream_writer
# Purpose: Writes records out to a stream
#
# Notes:
#
###############################################################################

from abc import ABCMeta, abstractmethod


class StreamWriter(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def write(self, record):
        """Write the given record to a stream
        :param object record: Record object to write out to stream
        """
        pass

    @abstractmethod
    def flush(self):
        """Flush records in buffer out to stream"""
        pass
