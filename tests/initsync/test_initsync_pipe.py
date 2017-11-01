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
import datetime
import importlib
import logging
import os
import pytest
import tests.unittest_utils as unittest_utils

import data_pipeline.constants.const as const
import data_pipeline.initsync.factory as db_factory
import data_pipeline.initsync_pipe as initsync_pipe
import data_pipeline.sql.utils as sql_utils
import data_pipeline.utils.dbuser as dbuser
import data_pipeline.utils.utils as utils

from .data_build_colname_sql import (ORACLE_SOURCE_COLNAME_SQL,
                                     ORACLE_TARGET_COLNAME_SQL,
                                     MSSQL_SOURCE_COLNAME_SQL,
                                     MSSQL_TARGET_COLNAME_SQL,
                                     POSTGRES_SOURCE_COLNAME_SQL,
                                     POSTGRES_TARGET_COLNAME_SQL,
                                     GREENPLUM_SOURCE_COLNAME_SQL,
                                     GREENPLUM_TARGET_COLNAME_SQL)

from .data_table_exists import (ORACLE_TABLE_EXISTS_SQL,
                                POSTGRES_TABLE_EXISTS_SQL,
                                MSSQL_TABLE_EXISTS_SQL)


from mock import Mock, MagicMock
from data_pipeline.initsync.exceptions import NotSupportedException
from data_pipeline.audit.custom_orm import (ProcessControl,
                                            ProcessControlDetail,
                                            SourceSystemProfile)


TEST_AUDIT_SCHEMA = 'foo'


DELETE_SQL = """
        DELETE FROM myschema.mytable
        WHERE 1=1"""

DELETE_QUERY_CONDITION_SQL = """
        DELETE FROM myschema.mytable
        WHERE 1=1
          AND col1 like '%foo%'"""

TRUNCATE_SQL = "TRUNCATE myschema.mytable"


def load_tests(name):
    # Load module which contains test data
    tests_module = importlib.import_module(name)
    # Tests are to be found in the variable `tests` of the module
    for test in tests_module.tests:
        yield test


def pytest_generate_tests(metafunc):
    """ This allows us to load tests from external files by
    parametrizing tests with each test case found in a data_X
    file """
    for fixture in metafunc.fixturenames:
        if fixture.startswith('data_'):
            # Load associated test data
            current_package = __name__.rpartition('.')[0]
            tests = load_tests("{}.{}".format(current_package, fixture))
            metafunc.parametrize(fixture, tests)


def default_execute_query_se(query, arraysize, values=(),
                             post_process_func=None):
    mock_query_results = None
    print("query executed='{}'".format(query))

    if (ORACLE_TABLE_EXISTS_SQL == query or
        MSSQL_TABLE_EXISTS_SQL == query):
        mock_query_results_config = {
            'next.return_value': [1],
            'fetchone.return_value': [1]
        }
        mock_query_results = Mock(**mock_query_results_config)
    elif POSTGRES_TABLE_EXISTS_SQL == query:
        mock_query_results_config = {
            'next.return_value': [True],
            'fetchone.return_value': [True]
        }
        mock_query_results = Mock(**mock_query_results_config)
    elif (ORACLE_SOURCE_COLNAME_SQL == query or
          ORACLE_TARGET_COLNAME_SQL == query or
          MSSQL_SOURCE_COLNAME_SQL == query or
          MSSQL_TARGET_COLNAME_SQL == query or
          POSTGRES_SOURCE_COLNAME_SQL == query or
          POSTGRES_TARGET_COLNAME_SQL == query or
          GREENPLUM_SOURCE_COLNAME_SQL == query or
          GREENPLUM_TARGET_COLNAME_SQL == query):
        mock_query_results_config = {
            '__iter__.return_value': [
                ['col1', 'VARCHAR'],
                ['/col2\\', 'INTEGER'],
                ['bitcol', 'BIT'],
            ]
        }
        mock_query_results = MagicMock(**mock_query_results_config)

    return mock_query_results


def build_mockdb(mocker, base_config, dbtype):
    db_config = utils.merge_dicts(base_config, {
        'dbtype': dbtype
    })
    return mocker.Mock(**db_config)


def build_initsyncdb(mocker, mockdb, argv, logger):
    mock_db_factory_build = mocker.patch('data_pipeline.initsync.factory.db_factory.build')
    mock_db_factory_build.return_value = mockdb
    return db_factory.build(mockdb.dbtype, argv, logger)


@pytest.fixture()
def setup(tmpdir, mocker):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv_config = utils.merge_dicts(mockargv_config, {
        'auditschema': TEST_AUDIT_SCHEMA
    })
    mockargv = mocker.Mock(**mockargv_config)

    unittest_utils.setup_logging(mockargv.workdirectory)

    mock_get_program_args = mocker.patch(
        'data_pipeline.initsync_pipe.get_program_args')
    mock_get_program_args.return_value = mockargv

    db_config = {
        'execute_query.side_effect': default_execute_query_se,
        'close.return_value': None,
    }

    logger = logging.getLogger(__name__)

    table = sql_utils.TableName('myschema', 'mytable')

    mock_process_control_constructor(mocker)
    mock_process_control_detail_constructor(mocker)

    yield (mockargv_config, db_config, table, logger, tmpdir)


def mock_process_control_constructor(mocker):
    mock_pc = build_mock_pc(mocker)
    mock_pc_constructor = mocker.patch(
        'data_pipeline.initsync_pipe.ProcessControl')
    mock_pc_constructor.return_value = mock_pc


def build_mock_pc(mocker):
    mock_pc_config = { }
    return mocker.Mock(**mock_pc_config)


def mock_process_control_detail_constructor(mocker):
    mock_pcd = build_mock_pcd(mocker)
    mock_pcd_constructor = mocker.patch(
        'data_pipeline.initsync_pipe.ProcessControlDetail')
    mock_pcd_constructor.return_value = mock_pcd


def build_mock_pcd(mocker):
    mock_pcd_config = {
        'insert.return_value': None,
        'update.return_value': None,
    }
    return mocker.Mock(**mock_pcd_config)


def test_table_exists(data_table_exists, mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup
    mockargv = mocker.Mock(**mockargv_config)

    dbtype = data_table_exists.input_dbtype
    expect_sql_call = data_table_exists.expect_sql_call 

    mockdb = build_mockdb(mocker, db_config, dbtype)
    db = build_initsyncdb(mocker, mockdb, mockargv, logger)

    assert db.table_exists(table)
    mockdb.execute_query.assert_called_once_with(
        expect_sql_call, const.DEFAULT_ARRAYSIZE)


def test_build_colname_sql(data_build_colname_sql, mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup
    mockargv = mocker.Mock(**mockargv_config)

    print("data_build_colname_sql={}".format(data_build_colname_sql))
    dbtype = data_build_colname_sql.input_dbtype
    is_source_db = data_build_colname_sql.input_is_source_db
    expect_sql_call = data_build_colname_sql.expect_sql_call
    expect_cols = data_build_colname_sql.expect_cols

    mockdb = build_mockdb(mocker, db_config, dbtype)
    db = build_initsyncdb(mocker, mockdb, mockargv, logger)
    if expect_sql_call:
        if is_source_db:
            cols = db.get_source_column_list(table)
        else:
            cols = db.get_target_column_list(table)

        mockdb.execute_query.assert_called_once_with(
            expect_sql_call, const.DEFAULT_ARRAYSIZE)
        assert cols == expect_cols
    else:
        with pytest.raises(NotSupportedException) as e:
            db.get_column_list(table, strip_control_chars)


def test_initsync_table(mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup

    mockargv_config = utils.merge_dicts(mockargv_config, {
        "sourcedbtype": const.ORACLE,
        "targetdbtype": const.POSTGRES,
    })
    mockargv = mocker.Mock(**mockargv_config)

    source_conn_detail = dbuser.get_dbuser_properties(mockargv.sourceuser)
    target_conn_detail = dbuser.get_dbuser_properties(mockargv.targetuser)
    source_schema = "myschema"
    target_schema = "myschema"
    process_control_id = 1
    query_condition = None

    mock_msg_queue_config = {"put.return_value": None}
    mock_msg_queue = mocker.Mock(**mock_msg_queue_config)

    mockdb = build_mockdb(mocker, db_config, const.ORACLE)
    db = build_initsyncdb(mocker, mockdb, mockargv, logger)

    mock_update_ssp = mocker.patch("data_pipeline.initsync_pipe._update_source_system_profile")
    # We don't want a real process being forked during tests
    mock_process = mocker.patch("data_pipeline.initsync_pipe.Process")

    initsync_pipe.initsync_table(mockargv, source_conn_detail,
        target_conn_detail, source_schema, table.name, target_schema,
        process_control_id, query_condition, mock_msg_queue)

    assert mock_update_ssp.call_count == 2


@pytest.mark.parametrize("raise_exception", [
    (False),
    (True),
])
def test_extract(raise_exception, mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup

    mockargv_config = utils.merge_dicts(mockargv_config, {
        "samplerows": 10,
    })
    mockargv = mocker.Mock(**mockargv_config)

    db_config = utils.merge_dicts(db_config, {
        "copy_expert.return_value": 99,
    })

    mock_extract_data = mocker.patch(
        "data_pipeline.initsync_pipe.ProcessControlDetail")

    #    if raise_exception:
    #        raise Exception("Failed to run mock_function")

    mockdb = build_mockdb(mocker, db_config, const.POSTGRES)
    db = build_initsyncdb(mocker, mockdb, mockargv, logger)

    pipe_file = os.path.join(mockargv.workdirectory, "myfakefifo")
    open(pipe_file, 'a').close()

    mock_pc_detail_config = {"update.return_value":None}
    mock_pc_detail = mocker.Mock(**mock_pc_detail_config)
    mock_pc_detail_cons = mocker.patch(
        "data_pipeline.initsync_pipe.ProcessControlDetail")
    mock_pc_detail_cons.return_value = mock_pc_detail

    mock_msg_queue_config = {"put.return_value": None}
    mock_msg_queue = mocker.Mock(**mock_msg_queue_config)

    initsync_pipe.extract(mockargv, pipe_file, table, ["col1", "col2"],
        db, 1, None, mock_msg_queue)

    mockdb.execute_query.assert_called_once_with("""
        SELECT
              col1
            , col2
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
            , 'UNSUPPORTED BY DB'
        FROM myschema.mytable
        WHERE 1=1
        LIMIT 10""", 1000, post_process_func=mocker.ANY)


def test_extract_data(data_extract_data, mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup

    column_list = data_extract_data.input_column_list
    dbtype = data_extract_data.input_dbtype
    extractlsn = data_extract_data.input_extractlsn
    samplerows = data_extract_data.input_samplerows
    lock = data_extract_data.input_lock
    query_condition = data_extract_data.input_query_condition
    expected_sql = data_extract_data.expected_sql
    metacols= data_extract_data.input_metacols

    mockargv_config = utils.merge_dicts(mockargv_config, {
        'lock': lock,
        'samplerows': samplerows,
        'extractlsn': extractlsn,
        'metacols': metacols,
    })
    mockargv = mocker.Mock(**mockargv_config)

    mockdb = build_mockdb(mocker, db_config, dbtype)
    db = build_initsyncdb(mocker, mockdb, mockargv, logger)

    def mock_function(argv, table, extract_data_sql):
        pass

    db.extract_data(column_list, table, query_condition, mock_function)

    # Assert called with SQL str
    mockdb.execute_query.assert_called_once_with(
        expected_sql, const.DEFAULT_ARRAYSIZE,
        post_process_func=mocker.ANY)


def test_create_pipe(mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup
    mockargv = mocker.Mock(**mockargv_config)

    file_path = initsync_pipe.create_pipe(mockargv, table, logger)

    assert file_path == os.path.join(mockargv.workdirectory,
                                     "{}.fifo".format(table.name))
    assert os.path.exists(file_path)


def test_get_output_filename(mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup
    mockargv = mocker.Mock(**mockargv_config)

    filename = initsync_pipe.get_output_filename(mockargv, table)

    assert filename == os.path.join(mockargv.workdirectory,
        "{}.{}".format(table.name, unittest_utils.TEST_OUTFILE))


def test_get_raw_filename(mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup
    mockargv = mocker.Mock(**mockargv_config)

    filename = initsync_pipe.get_raw_filename(mockargv, table)

    assert filename == os.path.join(mockargv.workdirectory,
        "{}.{}".format(table.name, unittest_utils.TEST_OUTFILE))


def test_report_initsync_summary(data_report_initsync_summary, mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup
    mockargv = mocker.Mock(**mockargv_config)
    mockpc = build_mock_pc(mocker)

    mock_send = mocker.patch("data_pipeline.initsync_pipe.mailer.send")

    assert not mock_send.called

    initsync_pipe.report_initsync_summary(
        mockargv, data_report_initsync_summary.input_all_table_results, mockpc,
        datetime.datetime.now(), datetime.datetime.now(), 1)

    mockpc.update.assert_called_once_with(
        total_count=data_report_initsync_summary.expected_total_count,
        min_lsn=data_report_initsync_summary.expected_min_lsn,
        max_lsn=data_report_initsync_summary.expected_max_lsn,
        comment="Completed InitSync",
        status=data_report_initsync_summary.expected_status,
        executor_run_id=data_report_initsync_summary.expected_run_id)

    (args, kwargs) = mock_send.call_args_list[0]
    assert kwargs['plain_text_message'] is not None
    assert kwargs['html_text_message'] is not None

    mock_send.assert_called_once_with(
        mockargv.notifysender,
        data_report_initsync_summary.expected_mailing_list,
        data_report_initsync_summary.expected_subject,
        mockargv.notifysmtpserver,
        plain_text_message=mocker.ANY,
        html_text_message=mocker.ANY)

@pytest.mark.parametrize("prev_status, curr_status, expected", [
    (const.SUCCESS, const.ERROR, const.WARNING),
    (const.ERROR, const.SUCCESS, const.WARNING),
    (None, const.SUCCESS, const.SUCCESS),
    (None, const.ERROR, const.ERROR),
    (const.ERROR, const.ERROR, const.ERROR),
    (const.SUCCESS, const.SUCCESS, const.SUCCESS),
])
def test_combine_statuses(prev_status, curr_status, expected, mocker, setup):
    assert initsync_pipe.combine_statuses(prev_status, curr_status) == expected


def test_vacuum_enabled(mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup
    mockargv_config = utils.merge_dicts(mockargv_config, {
        'vacuum': True,
        'analyze': False
    })
    mockargv = mocker.Mock(**mockargv_config)

    mockdb = build_mockdb(mocker, db_config, const.POSTGRES)
    db = build_initsyncdb(mocker, mockdb, mockargv, logger)

    table_name = sql_utils.TableName("ctl", "mytable")
    initsync_pipe.execute_post_processing(table_name, db)
    mockdb.execute.assert_called_with("VACUUM FULL ctl.mytable")


def test_analyze_enabled(mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup
    mockargv_config = utils.merge_dicts(mockargv_config, {
        'vacuum': False,
        'analyze': True 
    })
    mockargv = mocker.Mock(**mockargv_config)

    mockdb = build_mockdb(mocker, db_config, const.POSTGRES)
    db = build_initsyncdb(mocker, mockdb, mockargv, logger)

    table_name = sql_utils.TableName("ctl", "mytable")
    initsync_pipe.execute_post_processing(table_name, db)
    mockdb.execute.assert_called_with("ANALYZE ctl.mytable")


def test_report_no_active_schemas_tables(mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup
    mockargv = mocker.Mock(**mockargv_config)
    mockpc = build_mock_pc(mocker)

    initsync_pipe.report_no_active_schemas_tables(mockargv, mockpc, logger)
    mockpc.update.assert_called_with(comment=mocker.ANY, status=const.WARNING)


@pytest.mark.parametrize("query_condition, delete, truncate, expect_sql", [
    (None, True, False, DELETE_SQL),
    ("col1 like '%foo%'", True, False, DELETE_QUERY_CONDITION_SQL),
    (None, True, True, DELETE_SQL),
    ("col1 like '%foo%'", True, True, DELETE_QUERY_CONDITION_SQL),
    (None, False, True, TRUNCATE_SQL),
    (None, False, True, TRUNCATE_SQL),
])
def test_delete_truncate(query_condition, delete, truncate, expect_sql, mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup
    mockargv_config = utils.merge_dicts(mockargv_config, {
        'delete': delete,
        'truncate': truncate 
    })
    mockargv = mocker.Mock(**mockargv_config)

    mockdb = build_mockdb(mocker, db_config, const.POSTGRES)
    db = build_initsyncdb(mocker, mockdb, mockargv, logger)

    initsync_pipe.clean_target_table(mockargv, db, table, query_condition, 1)
    mockdb.execute.assert_called_once_with(expect_sql)


@pytest.mark.parametrize("nullstring", [
    (const.NULL),
    ("NA"),
])
def test_apply(nullstring, mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup

    mockargv_config = utils.merge_dicts(mockargv_config, {
        "nullstring": nullstring,
    })
    mockargv = mocker.Mock(**mockargv_config)

    db_config = utils.merge_dicts(db_config, {
        "copy_expert.return_value": 99,
    })

    mockdb = build_mockdb(mocker, db_config, const.POSTGRES)
    db = build_initsyncdb(mocker, mockdb, mockargv, logger)

    mock_msg_queue_config = {"put.return_value": None}
    mock_msg_queue = mocker.Mock(**mock_msg_queue_config)

    pipe_file = os.path.join(mockargv.workdirectory, "myfakefifo")
    open(pipe_file, 'a').close()

    mock_pc_detail_config = {"update.return_value":None}
    mock_pc_detail = mocker.Mock(**mock_pc_detail_config)
    mock_pc_detail_cons = mocker.patch(
        "data_pipeline.initsync_pipe.ProcessControlDetail")
    mock_pc_detail_cons.return_value = mock_pc_detail

    initsync_pipe.apply(mockargv, pipe_file, table, ["col1", "col2"],
        db, 1, mock_msg_queue)

    mockdb.copy_expert.assert_called_once_with(
        input_file=mocker.ANY,
        table_name="myschema.mytable",
        sep=const.FIELD_DELIMITER,
        null_string=nullstring,
        column_list=['col1', 'col2', 'ctl_ins_ts', 'ctl_upd_ts'],
        quote_char=chr(const.ASCII_GROUPSEPARATOR),
        escape_char=chr(const.ASCII_RECORDSEPARATOR),
        size=mockargv.buffersize
    )
    mock_pc_detail.update.assert_called_once_with(
        comment=mocker.ANY,
        status=const.SUCCESS,
        source_row_count=99,
        insert_row_count=99,
    )


@pytest.mark.parametrize("ssps, streamhost, streamchannel, seektoend_called_times", [
    ([("mysourceschema", "mytable", "mytargetschema", "col1 like '%foo%'")],
        "myhost", "mytopic", 1),
    ([("mysourceschema", "mytable", "mytargetschema", "col1 like '%foo%'")],
        "myhost", None, 0),
    ([("mysourceschema", "mytable", "mytargetschema", "col1 like '%foo%'")],
        None, "mytopic", 0),
    ([("mysourceschema", "mytable", "mytargetschema", "col1 like '%foo%'")],
        None, None, 0),
    (None, None, None, 0),
    ([], None, None, 0),
])
def test_main(ssps, streamhost, streamchannel, seektoend_called_times, mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup
    mockargv_config = utils.merge_dicts(mockargv_config, {
        'streamhost': streamhost,
        'streamchannel': streamchannel,
    })
    mockargv = mocker.Mock(**mockargv_config)

    def seek_to_end_se(timeout):
        print("kafka_consumer: seek_to_end called with timeout = {}"
              .format(timeout))
    mock_kafka_consumer_config = { "seek_to_end.side_effect": seek_to_end_se }
    mock_kafka_consumer = mocker.Mock(**mock_kafka_consumer_config)

    mock_get_program_args = mocker.patch(
        "data_pipeline.initsync_pipe.get_program_args")
    mock_get_program_args.return_value = mockargv

    mock_kafka_consumer_cons = mocker.patch(
        "data_pipeline.stream.factory.KafkaConsumer")
    mock_kafka_consumer_cons.return_value = mock_kafka_consumer

    mock_get_ssp = mocker.patch(
        "data_pipeline.initsync_pipe.get_source_system_profile_params")
    mock_get_ssp.return_value = ssps

    mock_parallelise_initsync = mocker.patch(
        "data_pipeline.initsync_pipe.parallelise_initsync")
    mock_parallelise_initsync.return_value = {
        table: ("LSN0", const.SUCCESS, "CCPI", "All good")
    }

    def report_error_se(message, process_control, logger):
        print("An error occurred in test: {}".format(message))
    mock_report_error = mocker.patch("data_pipeline.initsync_pipe.report_error")
    mock_report_error.size_effect = report_error_se

    mock_send = mocker.patch("data_pipeline.initsync_pipe.mailer.send")

    initsync_pipe.main()

    assert mock_kafka_consumer.seek_to_end.call_count == seektoend_called_times

    mock_report_error.assert_not_called()
    mock_send.assert_called_once()


@pytest.mark.parametrize("input_build_mock_pc", [
    (True),
    (False),
])
def test_report_error(input_build_mock_pc, mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup

    mock_pc = None
    if input_build_mock_pc:
        mock_pc = build_mock_pc(mocker)

    mockargv = mocker.Mock(**mockargv_config)

    mock_send = mocker.patch("data_pipeline.initsync_pipe.mailer.send")

    initsync_pipe.report_error(mockargv, "an error", mock_pc, logger)

    mock_send.assert_called_once_with(
        mockargv.notifysender,
        set(['someone@gmail.com', 'someone@error.com']),
        'myprofile InitSync ERROR',
        mockargv.notifysmtpserver,
        plain_text_message="an error"
    )

    if input_build_mock_pc:
        mock_pc.update.assert_called_once_with(
            comment="an error",
            status=const.ERROR
        )


@pytest.mark.parametrize("tablelist, isfile, expected_ssp_params", [
    ([], False, []),
    (["tableA"], False, [
        ("sys", "tableA", "ctl", None)]),
    (["tableA", "tableB"], False, [
        ("sys", "tableA", "ctl", None),
        ("sys", "tableB", "ctl", None)]),
    (["test.tabs"], True, [
        ("sys", "tableA", "ctl", None),
        ("sys", "tableB", "ctl", None),
        ("sys", "tableC", "ctl", None)]),
])
def test_get_source_system_profile_params_without_auditdb(
        tablelist, isfile, expected_ssp_params, mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup

    # Create a mock tablelist file
    if isfile:
        filename = str(tmpdir.mkdir("ssp").join(tablelist[0]))
        with open(filename, 'a') as f:
            f.write("tableA\n")
            f.write("tableB\n")
            f.write("tableC\n")

        tablelist = [filename]

    mockargv_config = utils.merge_dicts(mockargv_config, {
        'auditschema': None,
        'audituser': None,
        'tablelist': tablelist,
    })
    mockargv = mocker.Mock(**mockargv_config)

    ssp_params = initsync_pipe.get_source_system_profile_params(mockargv)
    assert ssp_params == expected_ssp_params


@pytest.mark.parametrize("nullstring, record, expected_payload", [
    (const.NULL, ('a', None, 99), "a{d}NULL\n".format(d=const.FIELD_DELIMITER)),
    ("NA", ('a', None, 99), "a{d}NA\n".format(d=const.FIELD_DELIMITER)),
])
def test_write(nullstring, record, expected_payload, mocker, setup):
    (mockargv_config, db_config, table, logger, tmpdir) = setup

    mockargv_config = utils.merge_dicts(mockargv_config, {
        'nullstring': nullstring,
    })
    mockargv = mocker.Mock(**mockargv_config)

    mock_fifo_file = mocker.Mock(**{"write.return_value": None})

    lsn = initsync_pipe.write(mockargv, record, mock_fifo_file, None)

    mock_fifo_file.write.assert_called_once_with(expected_payload)
    assert lsn == 99
