###############################################################################
# Module:    base_message
# Purpose:   A common message class containing logic for serialising and
#            deserialising messages
#
###############################################################################

import data_pipeline.constants.const as const

from abc import ABCMeta, abstractmethod


class BaseMessage(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.reset()

    def reset(self):
        self.batch_id = const.EMPTY_STRING
        self.record_type = const.EMPTY_STRING
        self.record_count = 0
        self.table_name = const.EMPTY_STRING
        self.commit_lsn = const.EMPTY_STRING
        self.message_sequence = const.EMPTY_STRING

    def serialise(self):
        """Serialises this python object to a dictionary
        :return: Returns a dictionary representation of this object
        :rtype: dict
        """
        return self.__dict__

    def deserialise(self, message):
        """Applies the message to the attributes of this object
        :param dict message: input message in as a dict
        """
        self.reset()
        self.__dict__ = message

    def __str__(self):
        return str(self.serialise())
