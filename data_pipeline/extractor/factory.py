###############################################################################
# Module:  factory
# Purpose: Build concrete instances of extractors for specific source databases
#
# Notes:
#
###############################################################################

from data_pipeline.db.exceptions import UnsupportedDbTypeError

import data_pipeline.constants.const as const


def build(mode, db, argv, audit_factory):
    """Return the specific extractor instance given the dbtype_name"""
    if db.dbtype == const.ORACLE:
        from .oracle_cdc_extractor import OracleCdcExtractor
        return OracleCdcExtractor(db, argv, audit_factory)
    elif db.dbtype == const.MSSQL:
        from .mssql_cdc_extractor import MssqlCdcExtractor
        return MssqlCdcExtractor(db, argv, audit_factory)
    else:
        raise UnsupportedDbTypeError(db.dbtype)
