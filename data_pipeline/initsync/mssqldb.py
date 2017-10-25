###############################################################################
# Module:    mssqldb
# Purpose:   Contains mssql specific initsync functions
#
# Notes:
#
###############################################################################


import data_pipeline.constants.const as const

from .exceptions import NotSupportedException
from .sqldb import SqlDb


class MssqlDb(SqlDb):
    def __init__(self, argv, db, logger):
        super(MssqlDb, self).__init__(argv, db, logger)

    def _is_string_type(self, datatype):
        return 'CHAR' in datatype.upper()

    def _get_ascii_function(self):
        return 'CHAR'

    def _add_column_modifiers_for_other_datatypes(self, colname, datatype):
        if datatype.upper() == 'BIT':
            return "CAST({colname} AS TINYINT) AS {colname}".format(colname=colname)
        return colname

    def _is_valid_char_num(self, char_num):
        return True

    def _build_colname_sql(self, table, lowercase):
        colname_select = "[column_name]"
        if lowercase:
            colname_select = "LOWER({})".format(colname_select)
        sqlstr = ("""
        SELECT {colname_select}, [data_type]
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER([table_schema]) = LOWER('{source_schema}')
          AND LOWER([table_name]) = LOWER('{table_name}')
          AND UPPER([data_type]) NOT IN ('IMAGE')
        ORDER BY ORDINAL_POSITION""".format(
            colname_select=colname_select,
            source_schema=table.schema,
            table_name=table.name))
        return sqlstr

    def _pre_extract(self):
        pass

    def _post_extract(self, record):
        return record

    def _wrap_colname(self, colname):
        return '[{}]'.format(colname)

    def _collate(self, colname):
        return "{} COLLATE Latin1_General_BIN".format(colname)

    def _build_extract_data_sql(self, column_list, table, extractlsn,
                                samplerows, lock, query_condition):
        withnolock = const.EMPTY_STRING if lock else "WITH (NOLOCK)"
        top = const.EMPTY_STRING

        if samplerows > 0:
            top = "TOP {rows}".format(rows=samplerows)

        extractlsn_sql = const.EMPTY_STRING
        if extractlsn:
            extractlsn_sql = """
            , (SELECT CONVERT(VARCHAR(50), SYS.FN_CDC_GET_MAX_LSN(), 2)) AS LSN"""

        metacols_sql = self._build_metacols_sql()

        sql = """
        SELECT {top}
              {columns}{metacols_sql}{extractlsn_sql}
        FROM {table}
        {withnolock}
        WHERE 1=1""".format(top=top,
                            columns="\n            , ".join(column_list),
                            metacols_sql=metacols_sql,
                            extractlsn_sql=extractlsn_sql,
                            table=table.fullname,
                            withnolock=withnolock)

        sql = self._append_query_condition(sql, query_condition)
        return sql

    def table_exists(self, table):
        table_exists_sql = self._build_table_exists_sql(table)
        result = self._db.execute_query(
            table_exists_sql, const.DEFAULT_ARRAYSIZE)
        row = result.fetchone()

        return int(row[0]) > 0 if row is not None else False

    def _build_table_exists_sql(self, table):
        return """
        IF EXISTS (
          SELECT * FROM INFORMATION_SCHEMA.TABLES
          WHERE LOWER([table_schema]) = LOWER('{schema}')
          AND   LOWER([table_name]) = LOWER('{table_name}')
        )
          BEGIN
            SELECT 1
          END
        ELSE
          BEGIN
            SELECT 0
          END""".format(schema=table.schema, table_name=table.name)

    def bulk_write(self, **kwargs):
        raise NotSupportedException(
            "bulk_write currently not supported for Mssql")

    def _build_analyze_sql(self, table):
        raise NotSupportedException(
            "build_analyze_sql currently not supported for Mssql")

    def _build_truncate_sql(self, table):
        raise NotSupportedException(
            "build_truncate_sql currently not supported for Mssql")

    def _build_vacuum_sql(self, table):
        raise NotSupportedException(
            "_build_vacuum_sql currently not supported for Mssql")
