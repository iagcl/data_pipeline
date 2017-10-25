###############################################################################
# Module:    mssql_cdc_extractor
# Purpose:   Implements MSSQL specific Extraction logic
#
# Notes:
#
###############################################################################

from .cdc_extractor import CdcExtractor
from .exceptions import InvalidArgumentsError
from data_pipeline.db.mssqldb import MssqlDb
from data_pipeline.db.connection_details import ConnectionDetails
from data_pipeline.stream.mssql_message import MssqlMessage

import csv
import cStringIO
import data_pipeline.constants.const as const

COL_NAME_START_INDEX = 10


class MssqlCdcExtractor(CdcExtractor):

    def __init__(self, db, argv, audit_factory):
        super(MssqlCdcExtractor, self).__init__(db, argv, audit_factory)
        self._message = MssqlMessage()
        self.key_fields = const.EMPTY_STRING
        self.column_names = const.EMPTY_STRING
        
    def get_source_minmax_cdc_point(self, last_run_max_lsn):
        min_lsn = None
        max_lsn = None
        
        #increment the previous MAXLSN and get new MAXLSN!!
        sqldef = "\n".join([
           "DECLARE @from_lsn binary(10), @to_lsn binary(10);"
           , "SET @from_lsn = sys.fn_cdc_increment_lsn( CONVERT(VARBINARY(20), LTRIM(RTRIM('0x{PRIORENDSCN}')), 1));"
           , "SET @to_lsn  = sys.fn_cdc_get_max_lsn();"
           , "SELECT CONVERT(VARCHAR(50),@from_lsn,2), CONVERT(VARCHAR(50),@to_lsn,2)"])
        sqldef = sqldef.format(PRIORENDSCN=last_run_max_lsn)
        
        query_results = self._source_db.execute_query(sqldef)
        for record in query_results:
            min_lsn = record[0]
            max_lsn = record[1]
            
        if max_lsn == last_run_max_lsn:
            min_lsn = last_run_max_lsn
                      
        self._logger.debug("min_lsn={}, max_lsn={}".format(min_lsn, max_lsn))
        return (min_lsn, max_lsn)


    def poll_cdcs(self, start_lsn, end_lsn):
        super(MssqlCdcExtractor, self).poll_cdcs(start_lsn, end_lsn)

        # Assume all tables reside in only one schema
        if self._segowners:
            schema = list(self._segowners)[0]
        else:
            schema = const.MSSQL_DEFAULT_SCHEMA

        self._logger.debug("Looking for CDCs in tables={}".format(self._tables))

        for table in self._tables:
            if start_lsn and end_lsn:

                #table="{}_{}".format(self._argv.sourceschema, table)
                self._logger.info("Polling database CDC points: {} -> {}".format(start_lsn, end_lsn))
               
                #TODO consider further validations on start and end LSN numbers                
                selectsamplestr = const.EMPTY_STRING
                if (self._argv.samplerows):
                    selectsamplestr = "TOP " + str(self._argv.samplerows)

                sqldef = " ".join([
                             "SELECT {selectsample}"
                             ,      "'DML'                                     AS operation_type"
                             ,      ",__$operation                             AS operation_code"
                             ,      ",'{tablename}'                            AS table_name"
                             ,      ",sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS commit_time"
                             ,      ",CONVERT(VARCHAR(50), __$start_lsn, 2)    AS commit_lsn"
                             ,      ",CONVERT(VARCHAR(50), __$start_lsn, 2) + '.' + CONVERT(VARCHAR(50), __$seqval, 2)  AS statement_id"
                             ,      ",* "
                             ,"FROM   cdc.fn_cdc_get_all_changes_{tablename} (CONVERT(VARBINARY(20),0x{startlsn}, 1), CONVERT(VARBINARY(20),0x{endlsn}, 1), N'all'); "])

                sqldef = sqldef.format(selectsample=selectsamplestr, tablename=table, startlsn=start_lsn, endlsn=end_lsn)

                query_results = self._source_db.execute_query(sqldef)

                # get the column names once per table
                self.column_names = self.get_column_names(query_results)
                
                count = 0
                self._init_batch_id()
                self._write_sob_message(start_lsn)
                for rec in query_results:

                    count += 1
                    membuf = cStringIO.StringIO()
                    memwrt = csv.writer(membuf, delimiter=const.FIELD_DELIMITER)
                    memwrt.writerow(rec)

                    tmpbuf = membuf.getvalue().strip(const.CHAR_CRLF)
                    row = tmpbuf.split(const.FIELD_DELIMITER)

                    if (self._argv.rawfile):
                        self.write_to_rawfile(tmpbuf)

                    self.write(self._build_record_message(query_results, row))

                # construct an end of batch record and write it to kafka topic
                if query_results:
                    count -= 1
                    self._pcd.delta_startlsn = start_lsn
                    self._pcd.delta_endlsn = end_lsn
                    self._write_eob_message(table, count, end_lsn)

            else:
                self._logger.error("No start_lsn and msx_lsns were computed.")

    def _build_record_message(self, query_results, row):
        self._reset_message()

        self._message.operation_code     = str(row[query_results.get_col_index('operation_code')])
        self._message.operation_type     = str(row[query_results.get_col_index('operation_type')])
        self._message.table_name         = str(row[query_results.get_col_index('table_name')])
        self._message.statement_id       = str(row[query_results.get_col_index('statement_id')])
        self._message.commit_lsn         = str(row[query_results.get_col_index('commit_lsn')])
        self._message.commit_timestamp   = str(row[query_results.get_col_index('commit_time')])
        self._message.message_sequence   = ""
        self._message.column_names       = str(self.column_names)
        self._message.column_values      = const.FIELD_DELIMITER.join(str(val).decode(self._argv.clientencoding) for val in row[COL_NAME_START_INDEX:])
        self._message.primary_key_fields = str(self._keyfieldslist.setdefault(row[query_results.get_col_index('table_name')]))

        return self._message.serialise()

    
    def get_column_names(self, query_results):
        # Column names of the table starts at field 8 of query_results
        return const.FIELD_DELIMITER.join(query_results.get_col_names()[COL_NAME_START_INDEX:])


    def get_column_values(self, row):
        # Column values of the table starts at field 8 of query_results
        return const.FIELD_DELIMITER.join(map(lambda x: '"{}"'.format(x), str(row[COL_NAME_START_INDEX:]).decode(self._argv.clientencoding)))


    def build_keycolumnlist(self, schemas, tables):
        owner_filter = const.EMPTY_STRING
        if schemas:
            owner_filter = "AND    LOWER(col.table_schema) IN ({0})".format(const.COMMASPACE.join(map(lambda x: "'{}'".format(x.lower()), schemas)))

        table_filter = const.EMPTY_STRING
        if tables:
            table_filter = "AND    LOWER(tab.table_name) IN ({0})".format(const.COMMASPACE.join(map(lambda x: "'{}'".format(x.lower()), tables)))

        sqlstmt = " ".join([
              "SELECT LOWER(tab.table_name)  tabname"
            , "      ,LOWER(col.column_name) colname"
            , "FROM   INFORMATION_SCHEMA.TABLE_CONSTRAINTS tab"
            , "      ,INFORMATION_SCHEMA.KEY_COLUMN_USAGE  col"
            , "WHERE  tab.constraint_name   = col.constraint_name"
            , "AND    tab.constraint_type  IN ('PRIMARY KEY', 'UNIQUE KEY')"
            , owner_filter
            , table_filter
            , "ORDER BY 1, col.ordinal_position"])

        query_results = self._source_db.execute_query(sqlstmt)

        if query_results:
            keycolumnlist = None
            priortablename = None
            for tabname, colname in query_results.fetchall():
                tablename = str(tabname)
                columnname = str(colname)
                self._logger.debug("tablename={}, columnname={}, priortablename={}".format(tablename, columnname, priortablename))
                if priortablename is None:
                    priortablename = tablename
                    keycolumnlist = columnname
                elif tablename == priortablename:
                    keycolumnlist = keycolumnlist + const.KEYFIELD_DELIMITER + columnname
                else:
                    self._keyfieldslist[priortablename] = keycolumnlist
                    priortablename = tablename
                    keycolumnlist = columnname

            self._keyfieldslist[priortablename] = keycolumnlist
        
        for tabname in tables:
            tablename = str(tabname)
            if not self._keyfieldslist.has_key(tablename):
                self._keyfieldslist[tablename] = const.NO_KEYFIELD_STR

        self._logger.debug("keyfieldlist={}".format(self._keyfieldslist))
