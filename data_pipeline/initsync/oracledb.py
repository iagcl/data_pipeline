###############################################################################
# Module:    oracledb
# Purpose:   Contains oracle specific initsync functions
#
# Notes:
#
###############################################################################


import data_pipeline.constants.const as const

from .exceptions import NotSupportedException
from .sqldb import SqlDb


class OracleDb(SqlDb):
    def __init__(self, argv, db, logger):
        super(OracleDb, self).__init__(argv, db, logger)

    def _is_string_type(self, datatype):
        datatype = datatype.upper()
        return datatype in ['CLOB', 'NCLOB'] or 'CHAR' in datatype

    def _get_ascii_function(self):
        return 'CHR'

    def _add_column_modifiers_for_other_datatypes(self, colname, datatype):
        return colname

    def _is_valid_char_num(self, char_num):
        return True

    def _build_colname_sql(self, table, lowercase):
        colname_select = "column_name"
        if lowercase:
            colname_select = "LOWER({})".format(colname_select)
        sqlstr = ("""
        SELECT {colname_select}, data_type
        FROM ALL_TAB_COLUMNS
        WHERE OWNER      = UPPER('{source_schema}')
          AND TABLE_NAME = UPPER('{table_name}')
          AND DATA_TYPE NOT IN ('SDO_GEOMETRY', 'RAW', 'BLOB')
        ORDER BY COLUMN_ID""".format(
            colname_select=colname_select,
            source_schema=table.schema,
            table_name=table.name))

        return sqlstr

    def _pre_extract(self):
        self._db.execute(const.ALTER_NLS_DATE_FORMAT_COMMAND)
        self._db.execute(const.ALTER_NLS_TIMESTAMP_FORMAT_COMMAND)

    def _wrap_colname(self, colname):
        return '"{}"'.format(colname)

    def _collate(self, colname):
        return colname

    def _build_extract_data_sql(self, column_list, table, extractlsn,
                                samplerows, lock, query_condition):
        extractlsn_sql = const.EMPTY_STRING
        if extractlsn:
            extractlsn_sql = """
            , (SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER
               FROM DUAL) AS SYS_SOURCE_TRANSACTION_ID"""

        metacols_sql = self._build_metacols_sql()

        sql = """
        SELECT
              {columns}{metacols_sql}{extractlsn_sql}
        FROM {table}
        WHERE 1=1""".format(columns="\n            , ".join(column_list),
                            metacols_sql=metacols_sql,
                            extractlsn_sql=extractlsn_sql,
                            table=table.fullname)

        sql = self._append_query_condition(sql, query_condition)
        sql = self._append_samplerows(sql, samplerows)

        return sql

    def _append_samplerows(self, sql, samplerows):
        if samplerows > 0:
            sql += """
          AND ROWNUM <= {rows}""".format(rows=samplerows)

        return sql

    def table_exists(self, table):
        table_exists_sql = self._build_table_exists_sql(table)
        result = self._db.execute_query(table_exists_sql,
                                        const.DEFAULT_ARRAYSIZE)
        row = result.fetchone()

        return int(row[0]) > 0 if row is not None else False

    def _build_table_exists_sql(self, table):
        return ("""
        SELECT COUNT(*) AS CNT
        FROM ALL_OBJECTS
        WHERE OBJECT_TYPE IN ('TABLE','VIEW')
          AND OWNER       = UPPER('{schema}')
          AND OBJECT_NAME = UPPER('{table_name}')"""
                .format(schema=table.schema, table_name=table.name))

    def bulk_write(self, **kwargs):
        raise NotSupportedException(
            "bulk_write currently not supported for Oracle")

    def _build_analyze_sql(self, table):
        raise NotSupportedException(
            "build_analyze_sql currently not supported for Oracle")

    def _build_truncate_sql(self, table):
        raise NotSupportedException(
            "build_truncate_sql currently not supported for Oracle")

    def _build_vacuum_sql(self, table):
        raise NotSupportedException(
            "_build_vacuum_sql currently not supported for Oracle")
