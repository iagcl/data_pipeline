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
# Module:    utils
# Purpose:   Collection of functions used for sql statement parsing
#
# Notes:
#
###############################################################################

import data_pipeline.constants.const as const


class TableName(object):
    def __init__(self, schema, name):
        self.schema = schema
        self.name = name

        self.fullname = ".".join(filter(lambda x: x,
                                        [schema, name]))

    def __str__(self):
        return self.fullname


def build_field_params(params):
    if params:
        return "({})".format(const.COMMASPACE.join(params))
    return const.EMPTY_STRING


def build_field_string(value):
    return " {}".format(value if value else const.EMPTY_STRING)


def build_datatype_sql(datatype, params, datatype_mapper):
    if datatype_mapper:
        return datatype_mapper.get_target_datatype(datatype, params)

    return "{datatype}{params}".format(
        datatype=build_field_string(datatype),
        params=build_field_params(params)
    )


def _map_operation(operation):
    """Default function to map the operation to target"""
    return operation


def _build_base_ddl_entry_sql(entry, datatype_mapper):
    datatype = build_datatype_sql(entry[const.DATA_TYPE],
                                  entry[const.PARAMS],
                                  datatype_mapper)

    constraints = build_field_string(entry.setdefault(const.CONSTRAINTS,
                                     const.EMPTY_STRING))

    return ("{field_name}{data_type}{constraints}"
            .format(field_name=build_field_string(entry[const.FIELD_NAME]),
                    data_type=datatype,
                    constraints=constraints)).strip()


def _build_alter_entry_sql(entry, datatype_mapper, ddl_entry_func,
                           map_operation_func=_map_operation):
    return ("{operation} {entry}"
            .format(operation=map_operation_func(entry[const.OPERATION]),
                    entry=ddl_entry_func(entry,
                                         datatype_mapper)))


def _build_create_entry_sql(entry, datatype_mapper):
    return _build_base_ddl_entry_sql(entry, datatype_mapper)


def _add_table_schema(schema, table_name):
    if schema:
        return "{schema}.{table_name}".format(schema=schema,
                                              table_name=table_name)
    return table_name


def build_create_sql(create_statement, datatype_mapper=None, schema=None):
    if create_statement.entries:
        table_name = _add_table_schema(schema, create_statement.table_name)
        entries = (const.COMMASPACE
                   .join([_build_create_entry_sql(e, datatype_mapper)
                          for e in create_statement.entries]))

        return ("CREATE TABLE {table_name} ({entries})"
                .format(table_name=table_name, entries=entries))

    return const.EMPTY_STRING


def build_alter_sql(alter_statement, map_operation_func=_map_operation,
                    ddl_entry_func=_build_base_ddl_entry_sql,
                    datatype_mapper=None,
                    schema=None):
    if alter_statement.entries:
        table_name = _add_table_schema(schema, alter_statement.table_name)

        entry_strings = []
        for e in alter_statement.entries:
            entry_strings.append(
                _build_alter_entry_sql(e, datatype_mapper, ddl_entry_func,
                                       map_operation_func))

        entries = (const.COMMASPACE.join(entry_strings))

        return ("ALTER TABLE {table_name} {entries}"
                .format(table_name=table_name, entries=entries))

    return const.EMPTY_STRING


def build_insert_sql(insert_statement, schema=None,
                     special_char_replacement=const.SPECIAL_CHAR_REPLACEMENT):

    field_names = _get_insert_field_names(
        insert_statement,
        None)

    sql_field_names = _get_insert_field_names(
        insert_statement,
        special_char_replacement)

    field_values = [_build_insert_field_value(insert_statement, f)
                    for f in field_names]

    table_name = _add_table_schema(schema, insert_statement.table_name)
    sqlstr = ("INSERT INTO {table_name} ( {field_names} ) "
              "VALUES ( {field_values} )"
              .format(table_name=table_name,
                      field_names=const.COMMASPACE.join(sql_field_names),
                      field_values=const.COMMASPACE.join(field_values)))

    return sqlstr


def build_bulk_insert_sql(
        insert_statements, schema=None,
        special_char_replacement=const.SPECIAL_CHAR_REPLACEMENT):
    """Builds a bulk insert statement given the list of insert statements
       along with their accompanying lsns
    """
    if not insert_statements:
        return None

    table_name = None
    field_names = None
    sqlstr = "INSERT INTO {table_name} ( {field_names} ) VALUES\n"
    field_value_list = []

    sep = const.SPACE * 2
    first_loop = True
    for (insert_statement, commit_lsn, offset) in insert_statements:
        if first_loop:
            table_name = _add_table_schema(schema, insert_statement.table_name)

            field_names = _get_insert_field_names(
                insert_statement,
                None)

            sql_field_names = _get_insert_field_names(
                insert_statement,
                special_char_replacement)

            sqlstr = sqlstr.format(
                table_name=table_name,
                field_names=const.COMMASPACE.join(sql_field_names))

        field_values = [_build_insert_field_value(insert_statement, f)
                        for f in field_names]

        sqlstr += ("{sep}( {csv_values} ) -- lsn: {commit_lsn}, offset: "
                   "{offset}\n"
                   .format(sep=sep,
                           csv_values=const.COMMASPACE.join(field_values),
                           commit_lsn=commit_lsn,
                           offset=offset))
        if sep == const.SPACE * 2:
            sep = const.COMMASPACE

        first_loop = False

    return sqlstr


def _get_insert_field_names(insert_statement, special_char_replacement):
    field_names = list(insert_statement.get_fields())

    field_names.sort()

    if special_char_replacement:
        field_names = [replace_special_chars(f, special_char_replacement)
                       for f in field_names]

    return field_names


def _build_insert_field_value(insert_statement, field):
    return build_value_sql(insert_statement.get_value(field))


def default_update_field_filter(field):
    return True


def build_update_sql(
        update_statement, schema=None,
        filter_func=default_update_field_filter,
        special_char_replacement=const.SPECIAL_CHAR_REPLACEMENT):
    """Builds an update SQL statement based on the update_statement.
    :param Statement update_statement: Contains the details necessary to build
        the SQL statement
    :param str schema: The schema to prepend to the table name
    :param function filter_func: Given the field name, returns a boolean value
        determining whether if that field should be present in the SET clause
    :param str special_char_replacement: The char to replace any special chars
        with
    """
    sv = update_statement.set_values
    if sv:
        set_clause_kv_pairs = [
            _build_field_equals_value_sql(k, v, False, special_char_replacement)
            for (k, v) in sv.items() if filter_func(k)]

        if not set_clause_kv_pairs:
            return None

        table_name = _add_table_schema(schema, update_statement.table_name)
        sqlstr = "UPDATE {table_name} SET ".format(table_name=table_name)
        sqlstr += const.COMMASPACE.join(set_clause_kv_pairs)
        sqlstr += build_where_sql(update_statement, special_char_replacement)
        return sqlstr

    return const.EMPTY_STRING


def build_delete_sql(
        delete_statement, schema=None,
        special_char_replacement=const.SPECIAL_CHAR_REPLACEMENT):

    table_name = _add_table_schema(schema, delete_statement.table_name)
    sqlstr = "DELETE FROM {table_name}".format(table_name=table_name)
    sqlstr += build_where_sql(delete_statement, special_char_replacement)

    return sqlstr


def build_where_sql(where_statement, special_char_replacement):
    where_conditions = where_statement.conditions
    pk_list = where_statement.primary_key_list
    primary_keys_in_where_clause = len(pk_list) > 0
    for pk in pk_list:
        if pk not in where_conditions:
            primary_keys_in_where_clause = False

    if primary_keys_in_where_clause:
        where_clause_items = {
            field: value for field, value
            in where_conditions.iteritems()
            if field in pk_list
        }
    else:
        where_clause_items = where_statement.conditions

    sqlstr = const.EMPTY_STRING
    c = special_char_replacement
    if where_clause_items:
        sqlstr = " WHERE "
        sqlstr += " AND ".join([_build_field_equals_value_sql(f, v, True, c)
                                for (f, v) in where_clause_items.items()])
    return sqlstr


def build_key_field_list(buf):
    keyfieldlist = []
    if buf != const.NO_KEYFIELD_STR:
        keyfieldlist = buf.split(const.KEYFIELD_DELIMITER)
        return keyfieldlist
    else:
        return None


def build_field_value_list(fields, values, start_position):
    fieldlist = []
    valuelist = []
    fieldvaluelist = dict()

    if fields and values:
        fieldlist = fields.split(const.FIELD_DELIMITER)[start_position:]
        valuelist = values.split(const.FIELD_DELIMITER)[start_position:]
        for seq in range(len(fieldlist)):
            if valuelist[seq]:
                fieldvaluelist[fieldlist[seq]] = valuelist[seq]
            else:
                fieldvaluelist[fieldlist[seq]] = None

        return fieldvaluelist


def escape_special_chars(value):
    if value:
        value = value.replace(const.SINGLE_QUOTE, const.SINGLE_QUOTE * 2)
        value = value.replace(const.BACKSLASH, const.BACKSLASH * 2)
        value = value.replace(const.PERCENT, const.PERCENT * 2)
    return value


def _is_metadata_current_time_sql(sql):
    stripped_sql = sql.strip()
    return stripped_sql == const.METADATA_CURRENT_TIME_SQL


def build_value_sql(value):
    if value is None:
        field_value = const.NULL
    elif _is_metadata_current_time_sql(value):
        field_value = value
    else:
        field_value = "'{value}'".format(value=escape_special_chars(value))

    return field_value


def _build_field_equals_value_sql(
        field_name, value, is_where_clause, special_char_replacement):

    if value is None and is_where_clause:
        operator = const.IS
    else:
        operator = const.EQUALS

    field_name = replace_special_chars(field_name, special_char_replacement)
    field_value = build_value_sql(value)

    return ("{field_name} {operator} {field_value}"
            .format(field_name=field_name,
                    operator=operator,
                    field_value=field_value))


def replace_special_chars(colname, replacement_char):
    if replacement_char:
        for special_char in const.COLUMN_NAME_SPECIAL_CHARS:
            colname = colname.replace(special_char,
                                      replacement_char)

    return colname
