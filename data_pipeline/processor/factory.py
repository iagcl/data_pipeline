###############################################################################
# Module:  factory
# Purpose: Build concrete instances of specific CDC processors
#
# Notes:
#
###############################################################################

import data_pipeline.constants.const as const

from data_pipeline.db.exceptions import UnsupportedDbTypeError
from data_pipeline.processor.oracle_cdc_processor import OracleCdcProcessor
from data_pipeline.processor.mssql_cdc_processor import MssqlCdcProcessor


def build(source_dbtype, metacols=None):
    """Return the specific type of CDC Processor object given the
    source_dbtype
    """
    if source_dbtype == const.ORACLE:
        return OracleCdcProcessor(metacols)
    elif source_dbtype == const.MSSQL:
        return MssqlCdcProcessor()
    else:
        raise UnsupportedDbTypeError(source_dbtype)
