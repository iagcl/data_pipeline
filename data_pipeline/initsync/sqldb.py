###############################################################################
# Module:    sqldb
# Purpose:   Represents an abstract SQL database
#
# Notes:
#
###############################################################################

import data_pipeline.sql.utils as sql_utils
import data_pipeline.constants.const as const

from initsyncdb import InitSyncDb
from abc import ABCMeta, abstractmethod


# ASCII chars to strip from initsync
STRIP_ASCII_CHARCODES = [
    const.ASCII_NULL,
    const.ASCII_STARTOFTEXT,
    const.ASCII_NEWLINE,
    const.ASCII_CARRIAGERETURN,
    const.ASCII_GROUPSEPARATOR,
    const.ASCII_RECORDSEPARATOR
]


class SqlDb(InitSyncDb):
    def __init__(self, argv, db, logger):
        super(SqlDb, self).__init__(argv, db, logger)

    def get_source_column_list(self, table):
        colname_sql = self._build_colname_sql(table, lowercase=False)
        result = self._db.execute_query(colname_sql, const.DEFAULT_ARRAYSIZE)

        column_list = []
        for row in result:
            colname = row[0]
            datatype = row[1]

            # Wrap colname to handle special characters in name
            colname = self._wrap_colname(colname)

            colname = self._add_column_modifiers(colname, datatype)

            column_list.append(colname)

        return column_list

    def get_target_column_list(self, table):
        colname_sql = self._build_colname_sql(table, lowercase=True)
        result = self._db.execute_query(colname_sql, const.DEFAULT_ARRAYSIZE)

        column_list = []
        for row in result:
            colname = sql_utils.replace_special_chars(
                colname=row[0],
                replacement_char=const.SPECIAL_CHAR_REPLACEMENT)
            column_list.append(colname)

        return column_list

    @abstractmethod
    def _build_colname_sql(self, table, lowercase):
        pass

    def _add_column_modifiers(self, colname, datatype):
        # Augment string values to make them suitable for applying to target
        if self._is_string_type(datatype):
            colname = self._collate(colname)

            copy_of_charcodes = STRIP_ASCII_CHARCODES[:]
            colname = self._wrap_with_ascii_replace(copy_of_charcodes, colname)

        # Defer handling of other datatypes to the child db types
        else:
            colname = self._add_column_modifiers_for_other_datatypes(colname,
                                                                     datatype)

        return colname

    @abstractmethod
    def _wrap_colname(self, colname):
        """Wraps the given column name with bounding characters to handle
        special characters within the column name
        """
        pass

    @abstractmethod
    def _collate(self, colname):
        """Adds a collate clause to the column to workaround a SQL Server bug:
        https://stackoverflow.com/questions/2298412/replace-null-character-in-a-string-in-sql
        """
        pass

    def _wrap_with_ascii_replace(self, ascii_list, body):
        if not ascii_list:
            return body

        ascii_char_num = ascii_list.pop()
        while not self._is_valid_char_num(ascii_char_num):
            if not ascii_list:
                return body
            ascii_char_num = ascii_list.pop()

        body = ("REPLACE({body}, {ascii_function}({num}), '')"
                .format(body=body,
                        ascii_function=self._get_ascii_function(),
                        num=ascii_char_num))

        return self._wrap_with_ascii_replace(ascii_list, body)

    @abstractmethod
    def _is_valid_char_num(self, char_num):
        pass

    @abstractmethod
    def _get_ascii_function(self):
        pass

    @abstractmethod
    def _is_string_type(self, datatype):
        pass

    @abstractmethod
    def _add_column_modifiers_for_other_datatypes(self, colname, datatype):
        pass

    def _build_metacols_sql(self):
        metacols_sql = ""

        ins_colname = self._argv.metacols.get(const.METADATA_INSERT_TS_COL)
        if ins_colname:
            metacols_sql += """
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp"""

        upd_colname = self._argv.metacols.get(const.METADATA_UPDATE_TS_COL)
        if upd_colname:
            metacols_sql += """
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp"""

        return metacols_sql

    @abstractmethod
    def _build_extract_data_sql(self, column_list, table, samplerows, lock):
        pass

    @abstractmethod
    def _pre_extract(self):
        pass

    def _post_extract(self, record):
        return record

    def _append_query_condition(self, sql, query_condition):
        if query_condition:
            sql += """
          AND {query_condition}""".format(query_condition=query_condition)

        return sql

    def delete(self, table, query_condition):
        sql = self._build_delete_sql(table, query_condition)
        return self._db.execute(sql)

    def _build_delete_sql(self, table, query_condition):
        sql = """
        DELETE FROM {table}
        WHERE 1=1""".format(table=table.fullname)

        sql = self._append_query_condition(sql, query_condition)
        return sql

    def truncate(self, table):
        sql = self._build_truncate_sql(table)
        return self._db.execute(sql)

    @abstractmethod
    def _build_truncate_sql(self, table):
        pass

    def execute_post_processing(self, target_table):
        if self._argv.vacuum:
            self.vacuum(target_table)

        if self._argv.analyze:
            self.analyze(target_table)

    def vacuum(self, table):
        sql = self._build_vacuum_sql(table)
        self._logger.info("Executing: {sql}".format(sql=sql))
        self._db.execute(sql)

    @abstractmethod
    def _build_vacuum_sql(self, table):
        pass

    def analyze(self, table):
        sql = self._build_analyze_sql(table)
        self._logger.info("Executing: {sql}".format(sql=sql))
        self._db.execute(sql)

    @abstractmethod
    def _build_analyze_sql(self, table):
        pass
