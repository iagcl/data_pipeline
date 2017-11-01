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
# Module:   args
# Purpose:  Module defining all switches and arguments used by the following
#           components:
#
#           - InitSync
#           - CDCExtract
#           - CDCApply
#
# Notes:
#
###############################################################################

import json
import os
import sys
import configargparse
import multiprocessing
import data_pipeline.constants.const as const
import data_pipeline.utils.filesystem as filesystem_utils
import data_pipeline.utils.utils as utils

from pprint import PrettyPrinter


def positive_int_type(x):
    x = int(x)
    if x < 0:
        raise configargparse.ArgumentTypeError("A negative number was supplied")
    return x


def commitpoint_type(x):
    x = int(x)
    if x < const.MIN_COMMIT_POINT:
        raise configargparse.ArgumentTypeError(
            "Minimum allowed commitpoint is: {}"
            .format(const.MIN_COMMIT_POINT))

    return x


class AppendDateTimeDirAction(configargparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        super(AppendDateTimeDirAction, self).__init__(option_strings,
                                                      dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        values = filesystem_utils.append_datetime_dir(values)
        setattr(namespace, self.dest, values)


def _is_initsync(mode):
    return (mode == const.INITSYNC or
            mode == const.INITSYNCEXTRACT or
            mode == const.INITSYNCAPPLY)


def _is_extract(mode):
    return mode == const.CDCEXTRACT


def _is_apply(mode):
    return mode == const.CDCAPPLY


def parse_args(arg_list, mode):

    common_args_parser = configargparse.ArgumentParser(
        config_file_parser_class=configargparse.YAMLConfigFileParser,
        usage='%(prog)s [options]',
        add_help=False)

    common_args_parser.add_argument(
        "-c", "--config",
        is_config_file=True,
        help="config file path")
    common_args_parser.add_argument(
        "--quiet",
        action="store_true",
        help="quiet mode")
    common_args_parser.add_argument(
        "--verbose",
        action="store_true",
        help="verbose mode")
    common_args_parser.add_argument(
        "--veryverbose",
        action="store_true",
        help="very verbose mode")
    common_args_parser.add_argument(
        "--tempdirectory",
        nargs='?',
        help="temporary working directory")
    common_args_parser.add_argument(
        "--workdirectory",
        nargs='?',
        action=AppendDateTimeDirAction,
        help="output working directory")
    common_args_parser.add_argument(
        "--audituser",
        nargs='?',
        help=("process audit user credentials requried for logging processing "
              "metrics"))
    common_args_parser.add_argument(
        "--auditschema",
        nargs='?',
        default='',
        help="schema name containing process audit tables")
    common_args_parser.add_argument(
        "--streamchannel",
        nargs='?',
        help="stream channel name / kafka topic")
    common_args_parser.add_argument(
        "--streamgroup",
        nargs='?',
        help="stream group identifer / kafka consumer group ")
    common_args_parser.add_argument(
        "--streamhost",
        nargs='?',
        help="stream host name / kafka cluster host")
    common_args_parser.add_argument(
        "--streamschemahost",
        nargs='?',
        help="stream schema host name / kafka cluster host")
    common_args_parser.add_argument(
        "--streamschemafile",
        nargs='?',
        help="stream schema file")
    common_args_parser.add_argument(
        "--dateformat",
        nargs='?',
        default='dd-mon-yyyy',
        help="date format mask used for data extraction")
    common_args_parser.add_argument(
        "--timeformat",
        nargs='?',
        default='hh24:mi:ss',
        help="time format mask used for data extraction")
    common_args_parser.add_argument(
        "--timestampformat",
        nargs='?',
        default='dd-mon-yyyy hh24:mi:ss',
        help="timestamp format mask used for data extraction")
    common_args_parser.add_argument(
        "--outputfile",
        nargs='?',
        help=("name of file where data is written to prior to being sent to "
              "an external source"))
    common_args_parser.add_argument(
        "--rawfile",
        nargs='?',
        help="name of file where raw extract source output is written to")
    common_args_parser.add_argument(
        "--tablelist",
        nargs='+',
        help="list of table(s) or file contain table list")
    common_args_parser.add_argument(
        "--profilename",
        nargs='?',
        help=("list of table(s) held as an application profile in process "
              "control database"))
    common_args_parser.add_argument(
        "--profileversion",
        nargs='?',
        type=float,
        help="application profile version number")
    common_args_parser.add_argument(
        "--delimiter",
        nargs='?',
        default=const.DELIMITER,
        help="fields-data value separator character")
    common_args_parser.add_argument(
        "--sourcedbtype",
        nargs='?',
        default=const.ORACLE,
        choices=[const.FILE, const.ORACLE, const.MSSQL, const.POSTGRES, const.GREENPLUM],
        help="")
    common_args_parser.add_argument(
        "--samplerows",
        nargs='?',
        type=positive_int_type,
        help="process a row sample")
    common_args_parser.add_argument(
        "--clientencoding",
        nargs='?',
        default='utf-8',
        help="source data character encoding")
    common_args_parser.add_argument(
        "--donotload",
        action="store_true",
        help="do not stream data - used for debugging source extraction")
    common_args_parser.add_argument(
        "--donotsend",
        action="store_true",
        help="do not stream data - used for debugging source extraction")
    common_args_parser.add_argument(
        "--sourcesystem",
        nargs='?',
        help="source system identifier (code)")
    common_args_parser.add_argument(
        "--kill",
        action="store_true",
        default=False,
        help=("Insert a poison pill down into kafka topic as a signal for "
              "consumers to exit gracefully"))
    common_args_parser.add_argument(
        "--auditcommitpoint",
        type=commitpoint_type,
        default=1000,
        help=("when looping over a large number of records, "
              "this will log audit updates every given number of records"))
    common_args_parser.add_argument(
        "--arraysize",
        nargs='?',
        type=int,
        default=const.DEFAULT_ARRAYSIZE,
        help=("this read-write attribute specifies the number of rows to "
              "fetch at a time internally and is the default number of rows "
              "to fetch with the fetchmany() call. Note this attribute can "
              "drastically affect the performance of a query since it "
              "directly affects the number of network round trips that need "
              "to be performed"))
    common_args_parser.add_argument(
        "--notifysmtpserver",
        nargs='?',
        default="localhost",
        help="hostname of the smtp server for sending emails")
    common_args_parser.add_argument(
        "--notifysender",
        nargs='?',
        default="data_pipleine@example.com",
        help="sender email address used when sending notification emails")
    common_args_parser.add_argument(
        "--notifyerrorlist",
        nargs='+',
        default=[],
        help=("space-separated list of recipient email addresses who will "
              "receive notifications upon an error in the application"))
    common_args_parser.add_argument(
        "--notifysummarylist",
        nargs='+',
        default=[],
        help=("space-separated list of recipient email addresses who will "
              "receive notifications of summary details and statistics upon "
              "completion of a run"))
    common_args_parser.add_argument(
        "--sslmode",
        choices=[
            const.SSLMODE_DISABLE,
            const.SSLMODE_ALLOW,
            const.SSLMODE_PREFER,
            const.SSLMODE_REQUIRE,
            const.SSLMODE_VERIFY_CA,
            const.SSLMODE_VERIFY_FULL,
        ],
        default=const.SSLMODE_PREFER,
        nargs='?',
        help=("use this sslmode for the database connection"))
    common_args_parser.add_argument(
        "--sslcert",
        nargs='?',
        default=("{home}/.postgresql/postgresql.crt"
                 .format(home=os.environ['HOME'])),
        help=("this parameter specifies the file name of the client SSL "
              "certificate"))
    common_args_parser.add_argument(
        "--sslrootcert",
        nargs='?',
        default=("{home}/.postgresql/root.crt"
                 .format(home=os.environ['HOME'])),
        help=("this parameter specifies the name of a file containing SSL "
              "certificate authority (CA) certificate(s)"))
    common_args_parser.add_argument(
        "--sslkey",
        nargs='?',
        default=("{home}/.postgresql/postgresql.key"
                 .format(home=os.environ['HOME'])),
        help=("this parameter specifies the location for the secret key used "
              "for the client certificate"))
    common_args_parser.add_argument(
        "--sslcrl",
        nargs='?',
        default=("{home}/.postgresql/root.crl"
                 .format(home=os.environ['HOME'])),
        help=("this parameter specifies the file name of the SSL certificate "
              "revocation list (CRL). Certificates listed in this file, if it "
              "exists, will be rejected while attempting to authenticate the "
              "server's certificate"))

    if _is_initsync(mode):
        initsync_args_parser = configargparse.ArgumentParser(
            prog=const.INITSYNC,
            add_help=True,
            parents=[common_args_parser])

        initsync_args_parser.add_argument(
            "--sourceuser",
            nargs='?',
            help=("source database user credentials in the form: "
                  "dbuser/dbpasswd@SRCSID[:PORT]"))
        initsync_args_parser.add_argument(
            "--sourceschema",
            nargs='?',
            help="schema name where source tables reside")
        initsync_args_parser.add_argument(
            "--sourceformat",
            nargs='?',
            default=const.TEXT,
            choices=[const.CSV, const.TEXT],
            help="format type of extracted data")
        initsync_args_parser.add_argument(
            "--loaddefinition",
            nargs='?',
            default=const.SRC,
            choices=[const.SRC, const.DEST],
            help="sql file or dictionary where ddl definition is extracted")
        initsync_args_parser.add_argument(
            "--parallelmode",
            nargs='?',
            type=int,
            default=2,
            help=("execute extraction query in parallel mode if supported by "
                  "database server"))
        initsync_args_parser.add_argument(
            "--querycondition",
            nargs='+',
            help="extra sql condition added to extraction query")
        initsync_args_parser.add_argument(
            "--sourcerowcount",
            action="store_true",
            help="get source row count")
        initsync_args_parser.add_argument(
            "--removectrlchars",
            nargs='?',
            help="remove control characters from extracted data")
        initsync_args_parser.add_argument(
            "--lock",
            action="store_true",
            help="execute extraction query without share lock")
        initsync_args_parser.add_argument(
            "--directunload",
            action="store_true",
            help=("use a direct unload utility to extract data from source - "
                  "mssql: bcp"))
        initsync_args_parser.add_argument(
            "--inputfile",
            nargs='?',
            help="full path to the stream data file - bypasses stream polling")
        initsync_args_parser.add_argument(
            "--targetdbtype",
            nargs='?',
            choices=[const.POSTGRES, const.GREENPLUM, const.JSON],
            help="")
        initsync_args_parser.add_argument(
            "--targetuser",
            nargs='?',
            help=("target database user credentials in the form: "
                  "dbuser/dbpasswd@SRCSID[:PORT]"))
        initsync_args_parser.add_argument(
            "--targetschema",
            nargs='?',
            help="schema name where target tables reside")
        initsync_args_parser.add_argument(
            "--datatypemap",
            nargs='?',
            help=("full path to the yaml config file containing the source->"
                  "target data type mappings"))
        initsync_args_parser.add_argument(
            "--numprocesses",
            nargs='?',
            type=int,
            default=multiprocessing.cpu_count(),
            help=("pool size of available processes for executing initsync. "
                  "A process will be dedicated for each table being synced"))
        initsync_args_parser.add_argument(
            "--buffersize",
            nargs='?',
            type=int,
            default=8192,
            help=("Size, in bytes, of the buffer used to read data into "
                  "before flushing to target"))
        initsync_args_parser.add_argument(
            "--extracttimeout",
            nargs='?',
            default=None,
            help=("Seconds to wait before timing out. By default, "
                  "initsync will wait indefinitely for extractor"))
        initsync_args_parser.add_argument(
            "--delete",
            action="store_true",
            help="Delete table records on target prior to initsync")
        initsync_args_parser.add_argument(
            "--truncate",
            action="store_true",
            help="Truncate tables on target prior to initsync")
        initsync_args_parser.add_argument(
            "--metacols",
            type=json.loads,
            default="{}",
            help=("enable metadata columns to be populated on target. "
                  "The format must be in json format (no spaces) of "
                  "supported metadata columns paired with their respective "
                  "target metadata column names. For example: "
                  "--metacols '{\"insert_timestamp_column\":\"ctl_ins_ts\","
                  "\"update_timestamp_column\":\"ctl_upd_ts\"}'. Supported metadata "
                  "columns are: "
                  "[insert_timestamp_column, update_timestamp_column]"))
        initsync_args_parser.add_argument(
            "--vacuum",
            action="store_true",
            help=("executes a VACCUM FULL on target DB "
                  "per table after successful apply"))
        initsync_args_parser.add_argument(
            "--analyze",
            action="store_true",
            help="executes an ANALYZE on target DB after successful apply")
        initsync_args_parser.add_argument(
            "--consumertimeout",
            nargs='?',
            default=const.KAFKA_DEFAULT_POLL_TIMEOUT,
            help=("Time to wait in blocking poll state while consuming "
                  "to end of queue at the end of an initsync"))
        initsync_args_parser.add_argument(
            "--extractlsn",
            action="store_true",
            help=("enables capturing of the source lsn at the point of "
                  "extract. Note, this will only be used on source "
                  "databases that support the concept of an "
                  "LSN/SCN/transactionid"))
        initsync_args_parser.add_argument(
            "--nullstring",
            nargs='?',
            default=const.NULL,
            help="the string used to identify a NULL value")

        parsed_args = initsync_args_parser.parse_args(arg_list)
        initsync_args_parser.print_values()

    elif _is_extract(mode):

        extract_args_parser = configargparse.ArgumentParser(
            prog=const.CDCEXTRACT,
            add_help=True,
            parents=[common_args_parser])

        extract_args_parser.add_argument(
            "--startscn",
            nargs='?',
            help="start scn or lsn for cdc transaction search")
        extract_args_parser.add_argument(
            "--endscn",
            nargs='?',
            help="end scn or lsn for cdc transaction search")
        extract_args_parser.add_argument(
            "--starttime",
            nargs='?',
            help="start time for cdc transaction search")
        extract_args_parser.add_argument(
            "--endtime",
            nargs='?',
            help="end time or lsn cdc for transaction search")
        extract_args_parser.add_argument(
            "--sourceschema",
            nargs='?',
            help="schema name where source tables reside")
        extract_args_parser.add_argument(
            "--sourcehost",
            nargs='?',
            help=("source server name or ip address containing application "
                  "database"))
        extract_args_parser.add_argument(
            "--sourceuser",
            nargs='?',
            help=("source database user credentials in the form: "
                  "dbuser/dbpasswd@SRCSID[:PORT]"))
        extract_args_parser.add_argument(
            "--scanlogs",
            nargs='?',
            type=int,
            help="number of archived logs to scan")
        extract_args_parser.add_argument(
            "--sourcedictionary",
            nargs='?',
            choices=[const.ONLINE_DICT, const.REDOLOG_DICT],
            default=const.ONLINE_DICT,
            help="source dictionary to use when running logminer")
        extract_args_parser.add_argument(
            "--checkexistingextract",
            action="store_true",
            help=("Enables checking of existing extracts with the same "
                  "profile name and version which are currently in progress. "
                  "Use this to prevent multiple extract process from running "
                  "concurrently."))
        extract_args_parser.add_argument(
            "--extractnewtables",
            action="store_true",
            help="Enables extraction of new create table DDLs")


        parsed_args = extract_args_parser.parse_args(arg_list)
        extract_args_parser.print_values()

    elif _is_apply(mode):

        applier_args_parser = configargparse.ArgumentParser(
            prog=const.CDCAPPLY,
            add_help=True,
            parents=[common_args_parser])
        applier_args_parser.add_argument(
            "--targetdbtype",
            nargs='?',
            choices=[const.POSTGRES, const.GREENPLUM, const.JSON],
            help="")
        applier_args_parser.add_argument(
            "--targethost",
            nargs='?',
            help=("target server name or ip address containing destination "
                  "database"))
        applier_args_parser.add_argument(
            "--targetuser",
            nargs='?',
            help=("target database user credentials in the form: "
                  "dbuser/dbpasswd@SRCSID[:PORT]"))
        applier_args_parser.add_argument(
            "--targetschema",
            nargs='?',
            help="schema name where target tables reside")
        applier_args_parser.add_argument(
            "--bulkapply",
            action="store_true",
            help=("Enables bulk applying of DML statements for improved "
                  "performance"))
        applier_args_parser.add_argument(
            "--bulkinsertlimit",
            nargs='?',
            type=int,
            default=50,
            help="max number of rows within a bulk insert")
        applier_args_parser.add_argument(
            "--insertnull",
            action="store_true",
            help="blank and empty strings are loaded as NULL values")
        applier_args_parser.add_argument(
            "--generatestatistics",
            action="store_true",
            help="generate table statistics after load")
        applier_args_parser.add_argument(
            "--donotapply",
            action="store_true",
            help=("do not apply parsed sql statments to target database - "
                  "for testing purposes"))
        applier_args_parser.add_argument(
            "--donotcommit",
            action="store_true",
            help=("do not commit sql statements to target database - for "
                  "testing purposes"))
        applier_args_parser.add_argument(
            "--inputfile",
            nargs='?',
            help=("full path to the stream data file - for testing purposes, "
                  "bypasses stream polling"))
        applier_args_parser.add_argument(
            "--datatypemap",
            nargs='?',
            help=("full path to the yaml config file containing the "
                  "source->target data type mappings"))
        applier_args_parser.add_argument(
            "--skipbatch",
            nargs='?',
            default=0,
            help=("skips the given number of batches without processing, "
                  "while still committing the offset"))
        applier_args_parser.add_argument(
            "--targetcommitpoint",
            type=commitpoint_type,
            default=1000,
            help=("when looping over a large number of records, this will "
                  "cause a commit of executed transactions on target "
                  "every given number of records"))
        applier_args_parser.add_argument(
            "--seektoend",
            action="store_true",
            help="Seek to the end of the kafka queue")
        applier_args_parser.add_argument(
            "--metacols",
            type=json.loads,
            default="{}",
            help=("enable metadata columns to be populated on target. "
                  "The format must be in json format (no spaces) of "
                  "supported metadata columns paired with their respective "
                  "target metadata column names. For example: "
                  "--metacols '{\"insert_timestamp_column\":\"ctl_ins_ts\","
                  "\"update_timestamp_column\":\"ctl_upd_ts\"}'. Supported metadata "
                  "columns are: "
                  "[insert_timestamp_column, update_timestamp_column]"))
        applier_args_parser.add_argument(
            "--retry",
            nargs='?',
            type=int,
            default=0,
            help=("When the applier exits due to an ERROR (environmental, "
                  "data etc), prior to exiting - retry execution from last "
                  "statement (the statement where error was detected) this "
                  "many times prior to exiting"))
        applier_args_parser.add_argument(
            "--retrypause",
            nargs='?',
            default=5,
            type=int,
            help="Pauses this number of seconds prior to retrying")

        parsed_args = applier_args_parser.parse_args(arg_list)
        applier_args_parser.print_values()
    else:
        parsed_args = None

    parsed_args = prefix_workdirectory_to_file_args(parsed_args)
    return parsed_args


def prefix_workdirectory_to_file_args(parsed_args):
    parsed_args = join_dir_and_file(parsed_args,
                                    parsed_args.workdirectory,
                                    'outputfile')
    parsed_args = join_dir_and_file(parsed_args,
                                    parsed_args.workdirectory,
                                    'rawfile')
    return parsed_args


def join_dir_and_file(parsed_args, directory, file_attribute_name):
    filename_arg = getattr(parsed_args, file_attribute_name)
    if filename_arg:
        filename = os.path.join(directory, filename_arg)
        filesystem_utils.ensure_path_exists(filename)
        setattr(parsed_args, file_attribute_name, filename)

    return parsed_args


def get_program_args(mode):
    argv = parse_args(sys.argv[1:], mode)
    pp = PrettyPrinter()
    print("Program input arguments:")
    print("========================")
    print(pp.pprint(argv.__dict__))
    return argv
