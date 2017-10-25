###############################################################################
# Module:    cdc_processor
# Purpose:   Processes CDCs polled from Kafka queue
#
# Notes:
#
###############################################################################

import logging

from abc import ABCMeta, abstractmethod


class Processor(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self._set_logger()

    def _set_logger(self):
        self._logger = logging.getLogger(__name__)

    def renew_workdirectory(self):
        self._set_logger()

    @abstractmethod
    def deserialise(self):
        pass

    @abstractmethod
    def process(self, stream_message):
        """Process CDC messsage into a statement
        :param dict stream_message: Stream message payload polled from queue
        :return: Statement object representing the statement to apply to target
        :rtype: Statement
        """
        pass
