###############################################################################
# Module:  factory
# Purpose: Build concrete instances of specific appliers
#
# Notes:
#
###############################################################################

import data_pipeline.constants.const as const
from data_pipeline.db.exceptions import UnsupportedDbTypeError


def build(mode, source_processor, db, argv, audit_factory):
    """Return the specific type of applier object given the dbtype_name"""
    if db.dbtype == const.POSTGRES:
        if mode == const.INITSYNCAPPLY:
            from data_pipeline.applier.postgres_initsync_applier import (
                PostgresInitSyncApplier
            )
            return PostgresInitSyncApplier(source_processor, db,
                                           argv, audit_factory)
        else:
            from data_pipeline.applier.postgres_cdc_applier import (
                PostgresCdcApplier
            )
            return PostgresCdcApplier(source_processor, db,
                                      argv, audit_factory)
    elif db.dbtype == const.GREENPLUM:
        from data_pipeline.applier.greenplum_cdc_applier import (
            GreenplumCdcApplier
        )
        return GreenplumCdcApplier(source_processor, db,
                                   argv, audit_factory)
    else:
        raise UnsupportedDbTypeError(db.dbtype)
