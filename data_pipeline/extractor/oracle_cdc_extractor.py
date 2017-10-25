###############################################################################
# Module:    oracle_cdc_extractor
# Purpose:   Implements Oracle specific Extraction logic, such as Logminer
#            specific logic
#
# Notes:
#
###############################################################################

import data_pipeline.constants.const as const

from .cdc_extractor import CdcExtractor
from .exceptions import InvalidArgumentsError
from data_pipeline.stream.oracle_message import OracleMessage


STOP_LOGMINER_COMMAND = "DBMS_LOGMNR.END_LOGMNR"


def build_where_in_list_filter(fieldname, values, negate=False):
    if not values:
        return const.EMPTY_STRING

    negation_str = "NOT " if negate else const.EMPTY_STRING

    csv_quoted_values = ','.join(["'{}'".format(x.upper()) for x in values])
    return ('{fieldname} {negation_str}IN ({values})'
            .format(fieldname=fieldname,
                    negation_str=negation_str,
                    values=csv_quoted_values))


class OracleCdcExtractor(CdcExtractor):

    def __init__(self, db, argv, audit_factory):
        super(OracleCdcExtractor, self).__init__(db, argv, audit_factory)
        self._message = OracleMessage()

    def get_source_minmax_cdc_point(self, last_run_max_scn):
        query = """
    SELECT MIN(log.firstchange) minscn, MAX(log.nextchange) maxscn
    FROM (
       SELECT NVL(first_change#, 0) firstchange,
              NVL(next_change#, 0) nextchange
       FROM v$archived_log
       WHERE 1 = 1
       AND   dest_id = 1 -- Prevent duplicate redo statements
       AND   first_change# >= {lastpoint}
       {filterlogs}
    ) log"""

        scanlogstr = const.EMPTY_STRING
        if (self._argv.scanlogs):
            scanlogstr = "AND rownum <= " + str(self._argv.scanlogs)

        query = query.format(lastpoint=last_run_max_scn, filterlogs=scanlogstr)
        query_results = self._source_db.execute_query(query,
                                                      self._argv.arraysize)

        min_scn = None
        max_scn = None

        for record in query_results:
            min_scn = record[query_results.get_col_index('MINSCN')]
            max_scn = record[query_results.get_col_index('MAXSCN')]

        return (max(min_scn, last_run_max_scn), max_scn)

    def poll_cdcs(self, start_scn, end_scn):
        self._records_written_to_stream = 0
        super(OracleCdcExtractor, self).poll_cdcs(start_scn, end_scn)
        if start_scn and end_scn:
            start_scn = long(start_scn)
            end_scn = long(end_scn)
            self._logger.info("Polling database CDC points: {} -> {}"
                              .format(start_scn, end_scn))
            if start_scn > end_scn:
                raise InvalidArgumentsError(
                    "Invalid state: Start CDC point {} > End {}"
                    .format(start_scn, end_scn))
            else:
                self._source_db.execute(
                    const.ALTER_NLS_DATE_FORMAT_COMMAND)
                self._source_db.execute(
                    const.ALTER_NLS_TIMESTAMP_FORMAT_COMMAND)

                start_logminer = self._build_logminer_start_command(
                    start_scn, end_scn, self._argv.sourcedictionary)
                self._source_db.execute_stored_proc(start_logminer)

                query_logminer = self._build_logminer_contents_query()
                query_results = self._source_db.execute_query(
                    query_logminer, self._argv.arraysize)

                self._init_batch_id()

                # No peek() function exposed by cursor API, so need to pick
                # off the first record to get the record count then process it
                row = query_results.fetchone()
                if row:
                    index = query_results.get_col_index(const.COUNT_FIELD)
                    rowcount = row[index]
                    self._write_sob_message(rowcount)
                    self._write_record(query_results, row, rowcount)
                    self._records_written_to_stream += 1

                    for row in query_results:
                        if self._sample_rows_reached():
                            break

                        self._write_record(query_results, row, rowcount)
                        self._records_written_to_stream += 1

                self._source_db.execute_stored_proc(STOP_LOGMINER_COMMAND)
        else:
            self._logger.error("No start/end scns were computed.")

    def _build_logminer_contents_query(self):
        base_query = """SELECT
          RTRIM(LTRIM(operation))                              AS {op_name}
        , RTRIM(LTRIM(rs_id))||'-'||ssn                        AS {sid_name}
        , TO_CHAR(COALESCE(cscn, scn))                         AS {cscn_name}
        , COALESCE(commit_timestamp, timestamp) || ''          AS {ts_name}
        , RTRIM(LTRIM({table_name}))                           AS {table_name}
        , TO_CHAR(csf)                                         AS {csf_name}
        , REPLACE(REPLACE(sql_redo, chr(13), ''), chr(10), '') AS {redo_name}
        , {segowner}
        , COUNT(*) OVER()                                      AS {count_name}
        FROM v$logmnr_contents
        """.format(
            op_name=const.OPERATION_FIELD,
            sid_name=const.STATEMENT_ID_FIELD,
            cscn_name=const.CSCN_FIELD,
            ts_name=const.SOURCE_TIMESTAMP_FIELD,
            table_name=const.TABLE_NAME_FIELD,
            csf_name=const.CSF_FLAG_FIELD,
            redo_name=const.SQLREDOSTMT_FIELD,
            segowner=const.SEG_OWNER_FIELD,
            count_name=const.COUNT_FIELD)

        extract_new_tables_sql = self._build_extract_new_tables_sql_predicate()

        query = """
        {base_query}
        WHERE 1 = 1
          AND {segowners_filter}
          AND {operation_filter}
          AND (
            -- Query for all REDO statements on known tables
            ({in_active_tables_filter})
            {extract_new_tables_sql}
          )
        """.format(
            base_query=base_query,
            operation_filter="operation IN ('INSERT','UPDATE','DELETE','DDL')",

            segowners_filter=build_where_in_list_filter(
                const.SEG_OWNER_FIELD, self._active_schemas),

            in_active_tables_filter=build_where_in_list_filter(
                const.TABLE_NAME_FIELD, self._active_tables),

            extract_new_tables_sql=extract_new_tables_sql
        )

        return query

    def _build_extract_new_tables_sql_predicate(self):

        if not self._argv.extractnewtables:
            return const.EMPTY_STRING

        redo_types = [const.INSERT, const.UPDATE, const.DELETE,
                      const.CREATE_TABLE, const.ALTER_TABLE]
        sqlified_redo_types = map(
            lambda x: "sql_redo LIKE '{redo_type_upper}%' OR "
                      "sql_redo LIKE '{redo_type_lower}%'"
                      .format(redo_type_upper=x.upper(),
                              redo_type_lower=x.lower()),
            redo_types
        )

        sql_redo_like_str = "\n                OR ".join(sqlified_redo_types)

        return """
            OR

            -- Query for any CREATE and DML statements for new tables only
            ({not_in_all_tables_filter}
              AND ({sql_redo_like_str}
              )
            )""".format(
            not_in_all_tables_filter=build_where_in_list_filter(
                const.TABLE_NAME_FIELD, self._all_tables, negate=True),
            sql_redo_like_str=sql_redo_like_str)

    def _build_logminer_start_command(
            self, start_scn, end_scn, sourcedictionary):

        base_options = [
            "DBMS_LOGMNR.COMMITTED_DATA_ONLY",
            "DBMS_LOGMNR.CONTINUOUS_MINE",
            "DBMS_LOGMNR.NO_ROWID_IN_STMT",
            "DBMS_LOGMNR.NO_SQL_DELIMITER",
            "DBMS_LOGMNR.SKIP_CORRUPTION"
        ]

        options = None

        if sourcedictionary == const.REDOLOG_DICT:
            self._logger.info("Searching for LogMiner dictionary archive logs")
            # Find the latest dictionary files before the start_scn
            dict_logs = self._get_last_dictionary_files(start_scn)

            # If no dictionary was found, use online dict
            if not dict_logs:
                self._logger.warn(
                    "No dictionary logs found. Going into "
                    "online dictionary mode")
                sourcedictionary = const.ONLINE_DICT
            else:
                self._logger.info("Found dictionary logs: {logs}"
                                  .format(logs=dict_logs))

                self._add_dictionary_archive_logs(dict_logs)

                options = base_options + [
                    "DBMS_LOGMNR.DICT_FROM_REDO_LOGS",
                    "DBMS_LOGMNR.DDL_DICT_TRACKING",
                ]

        if sourcedictionary == const.ONLINE_DICT:
            options = base_options + ["DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"]

        logminer_options = " + ".join(options)

        return """
        DBMS_LOGMNR.START_LOGMNR(
            STARTSCN => '{start_scn}',
            ENDSCN => '{end_scn}',
            OPTIONS => {options}
        )""".format(start_scn=start_scn,
                    end_scn=end_scn,
                    options=logminer_options)

    def _get_last_dictionary_files(self, start_next_extract_scn):
        sql = """
        SELECT name
        FROM V$ARCHIVED_LOG
        WHERE  1=1
        AND first_change# = (
            SELECT max(first_change#)
            FROM V$ARCHIVED_LOG
            WHERE DICTIONARY_BEGIN = 'YES'
              AND DICTIONARY_END   = 'YES' -- assume only complete dicts
              AND dest_id = 1 -- Prevent duplicate redo statements
              AND first_change#    < {scn})
        AND dest_id = 1 -- Prevent duplicate redo statements
            """.format(scn=start_next_extract_scn)

        query_results = self._source_db.execute_query(sql,
                                                      self._argv.arraysize)

        return [item for item in map(lambda r: r[0], query_results) if item]

    def _add_dictionary_archive_logs(self, dict_logs):
        logfilename_param = "LOGFILENAME => '{dict_log}'"
        new_dict_log_param = "OPTIONS => DBMS_LOGMNR.NEW"
        params = [logfilename_param, new_dict_log_param]
        new_dict_log_declared = False
        for dict_log in dict_logs:
            params_str = ", ".join(params).format(dict_log=dict_log)
            add_logfile_sql = ("DBMS_LOGMNR.ADD_LOGFILE({params})"
                               .format(params=params_str))

            self._source_db.execute_stored_proc(add_logfile_sql)
            if not new_dict_log_declared:
                params.pop()
                new_dict_log_declared = True

    def _sample_rows_reached(self):
        return (self._argv.samplerows and
                self._records_written_to_stream == self._argv.samplerows)

    def _write_record(self, query_results, row, rowcount):
        if (self._argv.rawfile):
            decoded_values = [
                str(val).decode(self._argv.clientencoding)
                for val in row
            ]
            tmpbuf = const.FIELD_DELIMITER.join(decoded_values)
            self.write_to_rawfile(tmpbuf)

        self.write(self._build_record_message(query_results, row,
                   rowcount))

    def _build_record_message(self, query_results, row, rowcount):
        self._reset_message()

        self._message.record_type = const.DATA

        self._message.record_count = rowcount

        self._message.operation_code = self._sanitise_value(
            row[query_results.get_col_index(const.OPERATION_FIELD)])

        self._message.table_name = self._sanitise_value(
            row[query_results.get_col_index(const.TABLE_NAME_FIELD)])

        self._message.commit_statement = self._sanitise_value(
            row[query_results.get_col_index(const.SQLREDOSTMT_FIELD)])

        self._message.statement_id = self._sanitise_value(
            row[query_results.get_col_index(const.STATEMENT_ID_FIELD)])

        self._message.commit_lsn = self._sanitise_value(
            row[query_results.get_col_index(const.CSCN_FIELD)])

        self._message.commit_timestamp = self._sanitise_value(
            row[query_results.get_col_index(const.SOURCE_TIMESTAMP_FIELD)])

        self._message.message_sequence = str(self._records_written_to_stream)

        self._message.multiline_flag = self._sanitise_value(
            row[query_results.get_col_index(const.CSF_FLAG_FIELD)])

        self._message.primary_key_fields = self._sanitise_value(
            self._keyfieldslist.setdefault(
                row[query_results.get_col_index(const.TABLE_NAME_FIELD)],
                const.NO_KEYFIELD_STR))

        return self._message.serialise()

    def _sanitise_value(self, value):
        """To prevent any None types from being written to kafka"""
        if value is None:
            return const.EMPTY_STRING
        return value

    def build_keycolumnlist(self, schemas, tables):
        owner_filter = const.EMPTY_STRING
        if schemas:
            schema_list = ["'{schema}'".format(schema=s.upper())
                           for s in schemas]
            csv_schemas = const.COMMASPACE.join(schema_list)
            owner_filter = ("AND UPPER(cons.owner) IN ({schemas})"
                            .format(schemas=csv_schemas))

        table_filter = const.EMPTY_STRING
        if tables:
            table_list = ["'{table_name}'".format(table_name=t.upper())
                          for t in tables]
            csv_tables = const.COMMASPACE.join(table_list)
            table_filter = ("AND UPPER(cons.table_name) IN ({tables})"
                            .format(tables=csv_tables))

        sqlstmt = " ".join([
            "SELECT cons.table_name tabname, LOWER(column_name) colname",
            "FROM   all_constraints cons, all_cons_columns col",
            "WHERE  cons.owner = col.owner",
            "AND    cons.constraint_name = col.constraint_name",
            owner_filter,
            table_filter,
            "AND   cons.constraint_type IN ('P', 'U')",
            "ORDER BY 1, col.position"])

        query_results = self._source_db.execute_query(sqlstmt,
                                                      self._argv.arraysize)

        keycolumnlist = None
        priortablename = None
        for tablename, columnname in query_results.fetchall():
            self._logger.debug("tablename={}, columnname={}, priortablename={}"
                               .format(tablename, columnname, priortablename))
            if priortablename is None:
                priortablename = tablename
                keycolumnlist = columnname
            elif tablename == priortablename:
                keycolumnlist = keycolumnlist + ',' + columnname
            else:
                self._keyfieldslist[priortablename] = keycolumnlist
                priortablename = tablename
                keycolumnlist = columnname

        self._keyfieldslist[priortablename] = keycolumnlist

        self._logger.debug("keyfieldlist={}".format(self._keyfieldslist))
