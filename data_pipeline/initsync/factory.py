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
# Module:    factory
# Purpose:   Builds the initsync endpoint modules for specific databases
#
# Notes:
#
###############################################################################

import importlib
import data_pipeline.db.factory as db_factory


def build(dbtype_name, argv, logger):
    db = db_factory.build(dbtype_name)

    module_name = "data_pipeline.initsync.{type}db".format(type=dbtype_name)
    module = importlib.import_module(module_name)

    class_name = "{upper}{rest}Db".format(upper=dbtype_name[0].upper(),
                                          rest=dbtype_name[1:])
    constructor = getattr(module, class_name)

    return constructor(argv, db, logger)
