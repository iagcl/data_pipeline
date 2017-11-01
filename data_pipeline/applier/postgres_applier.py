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
# Module:    postgres_applier
# Purpose:   Applies data acquired from stream to a Postgres DB
#
# Notes:     This module concerns itself mainly with generating Postgres SQL
#            for applying to target and mapping data types.
#
###############################################################################

import sys
import logging
import data_pipeline.constants.const as const
import data_pipeline.sql.utils as sql_utils

from abc import ABCMeta, abstractmethod
from .applier import Applier
from .exceptions import ApplyError


def _map_operation(operation):
    if operation == const.MODIFY:
        return const.ALTER
    return operation


def _build_ddl_entry(entry, datatype_mapper):
    """Builds a DDL entry string. For example: MYFIELD VARCHAR(400)
    :param dict entry: A dict of DDL entry details
    :param Applier datatype_mapper: An applier for the specific target
        which knows how to map from a generic datatype to target datatype
    """
    datatype = sql_utils.build_datatype_sql(entry[const.DATA_TYPE],
                                            entry[const.PARAMS],
                                            datatype_mapper)

    constraints = sql_utils.build_field_string(
        entry.setdefault(const.CONSTRAINTS,
                         const.EMPTY_STRING))

    type_modifier = const.EMPTY_STRING
    if entry[const.OPERATION] == const.MODIFY:
        type_modifier = " {t}".format(t=const.TYPE)

    field_name = sql_utils.build_field_string(entry[const.FIELD_NAME])
    return ("{field_name}{type_modifier}{data_type}{constraints}"
            .format(field_name=field_name,
                    type_modifier=type_modifier,
                    data_type=datatype,
                    constraints=constraints)).strip()


class PostgresApplier(Applier):
    __metaclass__ = ABCMeta

    def __init__(self, source_processor, target_db, argv, audit_factory):
        super(PostgresApplier, self).__init__(const.CDCAPPLY, target_db,
                                              argv, audit_factory,
                                              source_processor)

    @abstractmethod
    def _execute_statement(self, statement):
        """Execute statement on target
        :param Statement statement: The statement to execute on target
        """
        pass

    @abstractmethod
    def _commit_statements(self):
        """Commit all executed statements/transactions on target
        """
        pass

    def _build_insert_sql(self, insert_statement):
        return sql_utils.build_insert_sql(insert_statement,
                                          self._argv.targetschema)

    @abstractmethod
    def _build_update_sql(self, statement):
        """Builds postgres-like update sql statement.
        :param UpdateStatement statement: The representation of an
            Update statement containing all necessary details to
            create an Update SQL statement
        """
        pass

    def _build_delete_sql(self, delete_statement):
        return sql_utils.build_delete_sql(delete_statement,
                                          self._argv.targetschema)

    def _build_alter_sql(self, alter_statement):
        return sql_utils.build_alter_sql(alter_statement,
                                         _map_operation,
                                         _build_ddl_entry,
                                         self,
                                         self._argv.targetschema)

    def _build_create_sql(self, create_statement):
        return sql_utils.build_create_sql(create_statement, self,
                                          self._argv.targetschema)

    def _get_target_int_datatype(self, datatype_config, source_precision):
        """Maps the source integer precision to the equivalent
           target integer data type
        """
        rules = datatype_config.get(const.RULES, list())
        for rule in rules:
            precision_config = rule.get(const.PRECISION, None)
            if precision_config is not None:
                p_start = int(precision_config[const.PRECISION_START])
                p_end = int(precision_config[const.PRECISION_END])

                if source_precision >= p_start and source_precision <= p_end:
                    target_datatype = rule.get(const.TARGET, None)
                    keep_params = rule.get(const.KEEP_PARAMS, None)
                    return (target_datatype, keep_params)
        return None

    def get_target_datatype(self, datatype, params):
        if not datatype:
            return const.EMPTY_STRING

        source_target_config = self._config.get(self._argv.sourcedbtype, None)
        if source_target_config is None:
            raise ApplyError("There is no source db type '{}' defined in {}"
                             .format(self._argv.sourcedbtype,
                                     self._argv.datatypemap))

        datatype_config = source_target_config.get(datatype.lower(), None)

        if datatype_config is None:
            raise ApplyError("There is no {} data type '{}' defined in {}"
                             .format(self._argv.sourcedbtype,
                                     datatype.lower(), self._argv.datatypemap))

        target_datatype = None
        keep_params = True
        default = datatype_config.get(const.DEFAULT, None)
        if default:
            target_datatype = default.get(const.TARGET, None)
            keep_params = default.get(const.KEEP_PARAMS, True)

        source_precision = None
        source_scale = 0

        if params:
            if len(params) > 0:
                source_precision = int(params[const.PRECISION_PARAM_INDEX])
            if len(params) > 1:
                source_scale = int(params[const.SCALE_PARAM_INDEX])

        if source_scale == 0:
            tup = self._get_target_int_datatype(datatype_config,
                                                source_precision)
            if tup is not None:
                (target_datatype, keep_params) = tup

        # Last resort default
        if target_datatype is None:
            self._logger.warn("No target datatype was configured "
                              "for source datatype: {}. Defaulting to {}."
                              .format(datatype, const.DEFAULT_DATATYPE))
            target_datatype = const.DEFAULT_DATATYPE

        target_params = None
        if keep_params:
            target_params = params

        target_datatype_sql = "{datatype}{params}".format(
            datatype=sql_utils.build_field_string(target_datatype.upper()),
            params=sql_utils.build_field_params(target_params)
        )

        return target_datatype_sql
