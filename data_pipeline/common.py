###############################################################################
# Module:    common
# Purpose:   Contains shared functions used by initsync, extract and apply
#            modules
#
# Notes:
#
###############################################################################

from version import get_version
import logging
import signal
import sys
import data_pipeline.utils.args as args
import data_pipeline.utils.dbuser as dbuser
import data_pipeline.constants.const as const

from data_pipeline.utils.utils import merge_attributes
from data_pipeline.audit.audit_dao import (ProcessControl,
                                           ProcessControlDetail,
                                           SourceSystemProfile)


def set_process_control_schema(schema):
    ProcessControl.__table__.schema = schema
    ProcessControlDetail.__table__.schema = schema
    SourceSystemProfile.__table__.schema = schema


def get_program_args(mode):
    return args.get_program_args(mode)


def log_version(logger):
    logger.info("Version: data_pipeline_{version}"
                .format(version=get_version()))


class SignalHandler(object):
    def __init__(self, mode, argv, audit_factory):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self._mode = mode
        self._argv = argv
        self._logger = logging.getLogger(__name__)
        details = dbuser.get_dbuser_properties(self._argv.audituser)
        self._audit_conn_details = details
        self._audit_factory = audit_factory
        self._pc = audit_factory.build_process_control(self._mode)

    def exit_gracefully(self, signum, frame):
        self._logger.warn("\nApplication terminated with signal {sig}\n"
                          .format(sig=signum))
        self._pc.status = const.KILLED
        self._pc.update()
        sys.exit(signum)
