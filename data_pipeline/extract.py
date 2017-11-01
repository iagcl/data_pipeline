# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# 
###############################################################################
# Module:   extract
# Purpose:  Entry point for extractor program
#
# Notes:
#
###############################################################################

import logging
import data_pipeline.utils.dbuser as dbuser
import data_pipeline.constants.const as const
import data_pipeline.extractor.factory as extractor_factory
import data_pipeline.db.factory as db_factory
import data_pipeline.constants.const as const
import data_pipeline.logger.logging_loader as logging_loader

from .common import set_process_control_schema, get_program_args, log_version
from data_pipeline.audit.factory import AuditFactory


def get_source_db(argv):
    return db_factory.build(argv.sourcedbtype)


def build_extractor(mode, argv):
    db = get_source_db(argv)
    return extractor_factory.build(mode, db, argv, AuditFactory(argv))


def main():
    mode = const.CDCEXTRACT
    argv = get_program_args(mode)
    logging_loader.setup_logging(argv.workdirectory)
    logger = logging.getLogger(__name__)

    log_version(logger)

    set_process_control_schema(argv.auditschema)

    extractor = build_extractor(mode, argv)
    extractor.extract()


if __name__ == "__main__":
    main()
