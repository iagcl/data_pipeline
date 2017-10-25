###############################################################################
# Module:    postgresdb
# Purpose:   Contains postgres specific initsync functions
#
# Notes:
#
###############################################################################


import data_pipeline.constants.const as const

from .exceptions import NotSupportedException
from .sqldb import SqlDb


class PostgresDb(SqlDb):
    def __init__(self, argv, db, logger):
        super(PostgresDb, self).__init__(argv, db, logger)

    def _is_string_type(self, datatype):
        return 'CHAR' in datatype.upper()

    def _get_ascii_function(self):
        return 'CHR'

    def _add_column_modifiers_for_other_datatypes(self, colname, datatype):
        return colname

    def _is_valid_char_num(self, char_num):
        # Postgres errors on null char with: "null character not permitted"
        return char_num not in [const.ASCII_NULL]

    def _build_colname_sql(self, table, lowercase):
        colname_select = "column_name"
        if lowercase:
            colname_select = "LOWER({})".format(colname_select)
        sqlstr = ("""
        SELECT {colname_select}, data_type
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('{source_schema}')
          AND LOWER(table_name) = LOWER('{table_name}')
          AND UPPER(data_type) NOT IN ('IMAGE')
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
        return '"{}"'.format(colname)

    def _collate(self, colname):
        return colname

    def _build_extract_data_sql(self, column_list, table, extractlsn,
                                samplerows, lock, query_condition):

        extractlsn_sql = const.EMPTY_STRING
        if extractlsn:
            extractlsn_sql = """
            , 'UNSUPPORTED BY DB'"""

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
        LIMIT {rows}""".format(rows=samplerows)

        return sql

    def table_exists(self, table):
        table_exists_sql = self._build_table_exists_sql(table)
        result = self._db.execute_query(
            table_exists_sql,
            const.DEFAULT_ARRAYSIZE)
        row = result.fetchone()
        if row is None:
            return False

        (exists, ) = row
        return exists

    def _build_table_exists_sql(self, table):
        return """
        SELECT EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_schema = '{schema}'
              AND table_name   = '{table_name}'
        )""".format(schema=table.schema.lower(), table_name=table.name.lower())

    def bulk_write(self, **kwargs):
        return self._db.copy_expert(**kwargs)

    def _build_analyze_sql(self, table):
        return ("ANALYZE {full_table_name}"
                .format(full_table_name=table.fullname))

    def _build_truncate_sql(self, table):
        return ("TRUNCATE {full_table_name}"
                .format(full_table_name=table.fullname))

    def _build_vacuum_sql(self, table):
        return ("VACUUM FULL {full_table_name}"
                .format(full_table_name=table.fullname))
