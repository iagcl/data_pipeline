import os
import pytest
import data_pipeline.utils.args as args
import data_pipeline.extractor.extractor
import tests.unittest_utils as unittest_utils
import data_pipeline.utils.utils as utils
import data_pipeline.constants.const as const

from mock import Mock
from pytest_mock import mocker
from collections import deque
from data_pipeline.extractor.exceptions import InvalidArgumentsError
from data_pipeline.db.connection_details import ConnectionDetails
from data_pipeline.db.query_results import QueryResults
from data_pipeline.audit.audit_dao import (ProcessControl,
                                           ProcessControlDetail,
                                           SourceSystemProfile)
from data_pipeline.extractor.oracle_cdc_extractor import *

GET_DICTIONARY_QUERY = """
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
            """

STARTOF_MINMAX_QUERY = ("SELECT MIN(log.firstchange) minscn, "
                        "MAX(log.nextchange) maxscn")

BASE_LOGMNR_OPTIONS = [
    "DBMS_LOGMNR.COMMITTED_DATA_ONLY",
    "DBMS_LOGMNR.CONTINUOUS_MINE",
    "DBMS_LOGMNR.NO_ROWID_IN_STMT",
    "DBMS_LOGMNR.NO_SQL_DELIMITER",
    "DBMS_LOGMNR.SKIP_CORRUPTION"
]

ONLINE_DICT = ["DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"]

REDOLOG_DICT = [
    "DBMS_LOGMNR.DICT_FROM_REDO_LOGS",
    "DBMS_LOGMNR.DDL_DICT_TRACKING"
]

EXPECT_START_LOGMNR_CMD = """
        DBMS_LOGMNR.START_LOGMNR(
            STARTSCN => '{}',
            ENDSCN => '{}',
            OPTIONS => {}
        )"""


def build_logminer_options(options):
    return " + ".join(options)

@pytest.fixture()
def setup(tmpdir, mocker):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv_config = utils.merge_dicts(mockargv_config, {
        'donotload': False,
        'donotsend': False,
        'streamhost': 'host',
        'streamchannel': 'channel',
        'streamschemafile': 'file',
        'streamschemahost': 'host'})
    mockargv = mocker.Mock(**mockargv_config)
    unittest_utils.setup_logging(mockargv.workdirectory)

    db_config = {'execute_query.side_effect': default_execute_query_se}
    mockdb = mocker.Mock(**db_config)

    mock_audit_factory = unittest_utils.build_mock_audit_factory(mocker)

    unittest_utils.mock_get_prev_run_cdcs(mocker)
    mock_producer = unittest_utils.mock_build_kafka_producer(mocker)

    yield (OracleCdcExtractor(mockdb, mockargv, mock_audit_factory),
           mockdb, mockargv, mock_producer, 1, 2)


@pytest.fixture()
def setup_scn_override(tmpdir, mocker):
    ARGV_STARTSCN = 99
    ARGV_ENDSCN = 100
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv_config = utils.merge_dicts(mockargv_config, {
        'startscn': ARGV_STARTSCN,
        'endscn': ARGV_ENDSCN,
        })
    mockargv = mocker.Mock(**mockargv_config)

    unittest_utils.setup_logging(mockargv.workdirectory)

    unittest_utils.mock_get_schemas_and_tables(mocker, [], [])

    db_config = {'execute_query.side_effect': default_execute_query_se}
    mockdb = mocker.Mock(**db_config)

    mock_audit_factory = unittest_utils.build_mock_audit_factory(mocker)

    unittest_utils.mock_get_prev_run_cdcs(mocker)
    mock_producer = unittest_utils.mock_build_kafka_producer(mocker)

    yield (OracleCdcExtractor(mockdb, mockargv, mock_audit_factory),
           mockdb, mockargv, ARGV_STARTSCN, ARGV_ENDSCN)


@pytest.fixture()
def setup_redolog_dict(tmpdir, mocker):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv_config = utils.merge_dicts(mockargv_config, {
        'donotload': False,
        'donotsend': False,
        'streamhost': 'host',
        'streamchannel': 'channel',
        'streamschemafile': 'file',
        'streamschemahost': 'host',
        'sourcedictionary': 'redolog'})
    mockargv = mocker.Mock(**mockargv_config)
    unittest_utils.setup_logging(mockargv.workdirectory)

    db_config = {'execute_query.side_effect': default_execute_query_se}
    mockdb = mocker.Mock(**db_config)

    mock_audit_factory = unittest_utils.build_mock_audit_factory(mocker)

    unittest_utils.mock_get_prev_run_cdcs(mocker)
    mock_producer = unittest_utils.mock_build_kafka_producer(mocker)

    yield (OracleCdcExtractor(mockdb, mockargv, mock_audit_factory),
           mockdb, mockargv, mock_producer, 1, 2)


@pytest.fixture()
def setup_no_redolog_dict(tmpdir, mocker):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv_config = utils.merge_dicts(mockargv_config, {
        'donotload': False,
        'donotsend': False,
        'streamhost': 'host',
        'streamchannel': 'channel',
        'streamschemafile': 'file',
        'streamschemahost': 'host',
        'sourcedictionary': 'redolog'})
    mockargv = mocker.Mock(**mockargv_config)
    unittest_utils.setup_logging(mockargv.workdirectory)

    db_config = {'execute_query.side_effect': execute_query_no_redolog_se}
    mockdb = mocker.Mock(**db_config)

    mock_audit_factory = unittest_utils.build_mock_audit_factory(mocker)

    unittest_utils.mock_get_prev_run_cdcs(mocker)
    mock_producer = unittest_utils.mock_build_kafka_producer(mocker)

    yield (OracleCdcExtractor(mockdb, mockargv, mock_audit_factory),
           mockdb, mockargv, mock_producer, 1, 2)


@pytest.fixture()
def setup_kill(tmpdir, mocker):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv_config = utils.merge_dicts(mockargv_config, {
        'donotload': False,
        'donotsend': False,
        'kill': True})
    mockargv = mocker.Mock(**mockargv_config)
    unittest_utils.setup_logging(mockargv.workdirectory)

    db_config = {}
    mockdb = mocker.Mock(**db_config)

    mock_audit_factory = unittest_utils.build_mock_audit_factory(mocker)

    unittest_utils.mock_get_prev_run_cdcs(mocker)
    mock_producer = unittest_utils.mock_build_kafka_producer(mocker)

    yield (OracleCdcExtractor(mockdb, mockargv, mock_audit_factory),
           mockdb, mockargv, mock_producer)


def execute_query_no_redolog_se(query, arraysize):
    if STARTOF_MINMAX_QUERY in query:
        mock_query_results_config = {'get_col_index.return_value': 1}
        mock_query_results = Mock(**mock_query_results_config)
        mock_query_results.__iter__ = Mock(return_value=iter([[1, 2]]))
    elif "FROM v$logmnr_contents" in query:
        mock_row = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        mock_query_results_config = {'get_col_index.return_value': 1,
                                     'fetchone.return_value': mock_row}
        mock_query_results = Mock(**mock_query_results_config)
        mock_query_results.__iter__ = Mock(return_value=iter([mock_row]))

    elif "WHERE DICTIONARY_BEGIN = 'YES'" in query:
        redolog_name = None
        mock_row = [redolog_name]
        mock_query_results_config = {'get_col_index.return_value': 0,
                                     'fetchone.return_value': redolog_name}
        mock_query_results = Mock(**mock_query_results_config)
        mock_query_results.__iter__ = Mock(return_value=iter([mock_row]))
    else:
        mock_query_results_config = {
            'fetchall.return_value': [('schema', 'table')]
        }
        mock_query_results = Mock(**mock_query_results_config)
        mock_query_results.__iter__ = Mock(return_value=iter([]))
    return mock_query_results



def default_execute_query_se(query, arraysize):
    if STARTOF_MINMAX_QUERY in query:
        mock_query_results_config = {'get_col_index.return_value': 1}
        mock_query_results = Mock(**mock_query_results_config)
        mock_query_results.__iter__ = Mock(return_value=iter([[1, 2]]))
    elif "FROM v$logmnr_contents" in query:
        mock_row = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        mock_query_results_config = {'get_col_index.return_value': 1,
                                     'fetchone.return_value': mock_row}
        mock_query_results = Mock(**mock_query_results_config)
        mock_query_results.__iter__ = Mock(return_value=iter([mock_row]))

    elif "WHERE DICTIONARY_BEGIN = 'YES'" in query:
        redolog_name = 'my_redolog_name'
        mock_row = [redolog_name]
        mock_query_results_config = {'get_col_index.return_value': 0,
                                     'fetchone.return_value': redolog_name}
        mock_query_results = Mock(**mock_query_results_config)
        mock_query_results.__iter__ = Mock(return_value=iter([mock_row]))
    else:
        mock_query_results_config = {
            'fetchall.return_value': [('schema', 'table')]
        }
        mock_query_results = Mock(**mock_query_results_config)
        mock_query_results.__iter__ = Mock(return_value=iter([]))
    return mock_query_results


def test_terminate(mocker, setup_kill):
    (extractor, mockdb, mockargv, mock_producer) = setup_kill
    extractor.extract()
    mock_producer.write.assert_called_with({
        'multiline_flag': '',
        'primary_key_fields': '',
        'record_count': 0,
        'statement_id': '',
        'commit_lsn': '',
        'record_type': 'KILL',
        'table_name': '',
        'message_sequence': '',
        'batch_id': '',
        'commit_timestamp': '',
        'operation_code': '',
        'commit_statement': ''})


def test_poll_cdcs_specific_segowners_and_tables(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup

    unittest_utils.mock_get_schemas_and_tables(
        mocker, ['segA', 'segB'], ['tableA', 'tableB'])
    extractor.poll_cdcs(start_scn, end_scn)
    mockdb.assert_has_calls([
        mocker.call.execute(const.ALTER_NLS_DATE_FORMAT_COMMAND),
        mocker.call.execute(const.ALTER_NLS_TIMESTAMP_FORMAT_COMMAND),
        mocker.call.execute_stored_proc(
            EXPECT_START_LOGMNR_CMD.format(
                start_scn, end_scn, build_logminer_options(
                    BASE_LOGMNR_OPTIONS + ONLINE_DICT))),
        mocker.call.execute_query(mocker.ANY, 1000),
        mocker.call.execute_stored_proc(STOP_LOGMINER_COMMAND)
    ])


def test_poll_cdcs_specific_segowners_and_all_tables(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup
    unittest_utils.mock_get_schemas_and_tables(mocker, ['segA', 'segB'], [])
    extractor.poll_cdcs(start_scn, end_scn)
    mockdb.assert_has_calls([
        mocker.call.execute(const.ALTER_NLS_DATE_FORMAT_COMMAND),
        mocker.call.execute(const.ALTER_NLS_TIMESTAMP_FORMAT_COMMAND),
        mocker.call.execute_stored_proc(
            EXPECT_START_LOGMNR_CMD.format(
                start_scn, end_scn, build_logminer_options(
                    BASE_LOGMNR_OPTIONS + ONLINE_DICT))),
        mocker.call.execute_query(mocker.ANY, 1000),
        mocker.call.execute_stored_proc(STOP_LOGMINER_COMMAND)
    ])


def test_poll_cdcs_all_segowners_and_specific_tables(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup
    unittest_utils.mock_get_schemas_and_tables(
        mocker, [], ['tableA', 'tableB'])
    extractor.poll_cdcs(start_scn, end_scn)
    mockdb.assert_has_calls([
        mocker.call.execute(const.ALTER_NLS_DATE_FORMAT_COMMAND),
        mocker.call.execute(const.ALTER_NLS_TIMESTAMP_FORMAT_COMMAND),
        mocker.call.execute_stored_proc(
            EXPECT_START_LOGMNR_CMD.format(
                start_scn, end_scn, build_logminer_options(
                    BASE_LOGMNR_OPTIONS + ONLINE_DICT))),
        mocker.call.execute_query(mocker.ANY, 1000),
        mocker.call.execute_stored_proc(STOP_LOGMINER_COMMAND)
    ])


def test_poll_cdcs_all_segowners_and_tables(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup
    unittest_utils.mock_get_schemas_and_tables(mocker, [], [])
    extractor.poll_cdcs(start_scn, end_scn)
    mockdb.assert_has_calls([
        mocker.call.execute(const.ALTER_NLS_DATE_FORMAT_COMMAND),
        mocker.call.execute(const.ALTER_NLS_TIMESTAMP_FORMAT_COMMAND),
        mocker.call.execute_stored_proc(
            EXPECT_START_LOGMNR_CMD.format(
                start_scn, end_scn, build_logminer_options(
                    BASE_LOGMNR_OPTIONS + ONLINE_DICT))),
        mocker.call.execute_query(mocker.ANY, 1000),
        mocker.call.execute_stored_proc(STOP_LOGMINER_COMMAND)
    ])


def test_poll_cdcs_valid_start_end_scn(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup
    unittest_utils.mock_get_schemas_and_tables(mocker, [], [])
    extractor.poll_cdcs(start_scn, end_scn)
    mockdb.assert_has_calls([
        mocker.call.execute(const.ALTER_NLS_DATE_FORMAT_COMMAND),
        mocker.call.execute(const.ALTER_NLS_TIMESTAMP_FORMAT_COMMAND),
        mocker.call.execute_stored_proc(
            EXPECT_START_LOGMNR_CMD.format(
                start_scn, end_scn, build_logminer_options(
                    BASE_LOGMNR_OPTIONS + ONLINE_DICT))),
        mocker.call.execute_query(mocker.ANY, 1000),
        mocker.call.execute_stored_proc(STOP_LOGMINER_COMMAND)
    ])


def test_extract_source_data_overriden_start_end_scn(mocker, setup_scn_override):
    (extractor, mockdb, mockargv, start_scn, end_scn) = setup_scn_override
    print("startscn={}, endscn={}".format(mockargv.startscn, mockargv.endscn))

    f = mocker.patch.object(extractor, "poll_cdcs")
    extractor._extract_source_data()
    f.assert_called_once_with(start_scn, end_scn)


def test_redolog_dict(mocker, setup_redolog_dict):
    (extractor, mockdb, mockargv,
     mock_producer, start_scn, end_scn) = setup_redolog_dict

    unittest_utils.mock_get_schemas_and_tables(mocker, [], [])
    extractor.poll_cdcs(start_scn, end_scn)
    mockdb.assert_has_calls([
        mocker.call.execute(const.ALTER_NLS_DATE_FORMAT_COMMAND),
        mocker.call.execute(const.ALTER_NLS_TIMESTAMP_FORMAT_COMMAND),
        mocker.call.execute_query(GET_DICTIONARY_QUERY
                                  .format(scn=start_scn), 1000),
        mocker.call.execute_stored_proc("DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => 'my_redolog_name', OPTIONS => DBMS_LOGMNR.NEW)"),
        mocker.call.execute_stored_proc(
            EXPECT_START_LOGMNR_CMD.format(
                start_scn, end_scn, build_logminer_options(
                    BASE_LOGMNR_OPTIONS + REDOLOG_DICT))),
        mocker.call.execute_query(mocker.ANY, 1000),
        mocker.call.execute_stored_proc(STOP_LOGMINER_COMMAND)
    ])


def test_no_redolog_dict(mocker, setup_no_redolog_dict):
    (extractor, mockdb, mockargv,
     mock_producer, start_scn, end_scn) = setup_no_redolog_dict

    unittest_utils.mock_get_schemas_and_tables(mocker, [], [])
    extractor.poll_cdcs(start_scn, end_scn)
    mockdb.assert_has_calls([
        mocker.call.execute(const.ALTER_NLS_DATE_FORMAT_COMMAND),
        mocker.call.execute(const.ALTER_NLS_TIMESTAMP_FORMAT_COMMAND),
        mocker.call.execute_query(GET_DICTIONARY_QUERY
                                  .format(scn=start_scn), 1000),
        # If no redolog is found, then should go into online dict mode
        mocker.call.execute_stored_proc(
            EXPECT_START_LOGMNR_CMD.format(
                start_scn, end_scn, build_logminer_options(
                    BASE_LOGMNR_OPTIONS + ONLINE_DICT))),
        mocker.call.execute_query(mocker.ANY, 1000),
        mocker.call.execute_stored_proc(STOP_LOGMINER_COMMAND)
    ])


def test_build_keycolumnlist(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup

    query_results_config = {'fetchall.return_value': []}
    mock_query_results = mocker.Mock(**query_results_config)

    db_config = {'execute_query.return_value': mock_query_results}
    mockdb = mocker.Mock(**db_config)

    mock_audit_factory = unittest_utils.build_mock_audit_factory(mocker)

    extractor = OracleCdcExtractor(mockdb, mockargv, mock_audit_factory)

    extractor.build_keycolumnlist(['schemaA', 'schemaB'], ['tableA', 'tableB'])
    mockdb.assert_has_calls([
        mocker.call.execute_query(
            "SELECT cons.table_name tabname, LOWER(column_name) colname "
            "FROM   all_constraints cons, all_cons_columns col "
            "WHERE  cons.owner = col.owner "
            "AND    cons.constraint_name = col.constraint_name "
            "AND UPPER(cons.owner) IN ('SCHEMAA', 'SCHEMAB') "
            "AND UPPER(cons.table_name) IN ('TABLEA', 'TABLEB') "
            "AND   cons.constraint_type IN ('P', 'U') "
            "ORDER BY 1, col.position", 1000),
        mocker.call.execute_query(mocker.ANY).fetchall()
    ])


def test_poll_cdcs_start_gt_end_scn(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup
    unittest_utils.mock_get_schemas_and_tables(mocker, [], [])
    with pytest.raises(InvalidArgumentsError):
        extractor.poll_cdcs(2, 1)


def test_connect_to_source_db(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup
    extractor.connect_to_source_db()
    mockdb.connect.assert_called_once()


def test_write_string_should_raise_error(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup

    message = "{'a': 'a raw message'}"
    with pytest.raises(TypeError) as type_error:
        extractor._init_stream_output()
        extractor.write_to_stream(message)

    expect_errstr = "Message is not a dict type. Message type passed: str"
    assert str(type_error.value) == expect_errstr


def test_write_to_file(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup
    message = {'a': 'a raw message'}
    extractor.write(message)
    with open(mockargv.outputfile, 'r') as f:
        f.read() == message


@pytest.mark.parametrize(
    "logminer_contents_query_results, "
    "expected_record_count, "
    "expected_record_types", [
        ([], 0, []),
        ([[1, 2]], 1, [const.START_OF_BATCH, const.DATA, const.END_OF_BATCH])])
def test_successful_run(logminer_contents_query_results,
                        expected_record_count,
                        expected_record_types, mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup

    logminer_results_deque = deque(logminer_contents_query_results)
    def execute_query_se(query, arraysize):
        if STARTOF_MINMAX_QUERY in query:
            mock_query_results_config = {'get_col_index.return_value': 1}
            mock_query_results = Mock(**mock_query_results_config)
            mock_query_results.__iter__ = Mock(return_value=iter([[1, 2]]))
        elif "FROM v$logmnr_contents" in query:
            mock_query_results_config = {
                'get_col_index.return_value': 1,
                'fetchone.return_value': (
                    None if not logminer_results_deque
                    else logminer_results_deque.popleft()
                )
            }
            mock_query_results = Mock(**mock_query_results_config)
            # Mock a single result with two columns returned
            mock_query_results.__iter__ = Mock(
                return_value=iter(logminer_results_deque))
        else:
            mock_query_results_config = {
                'fetchall.return_value': [('schema', 'table')]
            }
            mock_query_results = Mock(**mock_query_results_config)
            mock_query_results.__iter__ = Mock(return_value=iter([]))
        return mock_query_results

    mockdb.execute_query.side_effect = execute_query_se

    expected_record_types_deque = deque(expected_record_types)

    def write_se(message):
        if message['record_type'] == const.END_OF_BATCH:
            assert message['record_count'] == expected_record_count

        assert len(expected_record_types_deque) != 0
        rt = expected_record_types_deque.popleft()
        print("actual={}, expected={}".format(message['record_type'], rt))
        assert message['record_type'] == rt

        return True

    mock_producer.write.side_effect = write_se
    unittest_utils.mock_get_schemas_and_tables(
        mocker, ['MYSCHEMA'], ['MYTABLE'])

    extractor.extract()

    if len(expected_record_types_deque) > 0:
        pytest.fail("The remaining message record types were not sent: {}"
                    .format(expected_record_types_deque))


def test_extract_no_profile(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup
    unittest_utils.mock_get_schemas_and_tables(mocker, list(), list())

    with pytest.raises(InvalidArgumentsError) as invalid_args_error:
        extractor.extract()

    expect_errstr = ("Both schemas ([]) and tables ([]) must be defined "
                     "in source_system_profile for profile 'myprofile'")
    assert str(invalid_args_error.value) == expect_errstr


def test_extract_no_schemas_or_tables(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup
    unittest_utils.mock_get_schemas_and_tables(mocker, list(), list())

    with pytest.raises(InvalidArgumentsError) as invalid_args_error:
        extractor.extract()

    expect_errstr = ("Both schemas ([]) and tables ([]) must be defined "
                     "in source_system_profile for profile 'myprofile'")
    assert expect_errstr in str(invalid_args_error.value)


def test_extract_no_schemas_with_tables(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup
    unittest_utils.mock_get_schemas_and_tables(mocker, list(), ['MYTABLE'])

    with pytest.raises(InvalidArgumentsError) as invalid_args_error:
        extractor.extract()

    expect_errstr = ("Both schemas ([]) and tables (['MYTABLE']) must be "
                     "defined in source_system_profile for profile "
                     "'myprofile'")
    assert expect_errstr in str(invalid_args_error.value)


def test_extract_with_schemas_no_tables(mocker, setup):
    (extractor, mockdb, mockargv, mock_producer, start_scn, end_scn) = setup
    unittest_utils.mock_get_schemas_and_tables(mocker, ['MYSCHEMA'], list())

    with pytest.raises(InvalidArgumentsError) as invalid_args_error:
        extractor.extract()

    expect_errstr = ("Both schemas (['MYSCHEMA']) and tables ([]) must be "
                     "defined in source_system_profile for profile "
                     "'myprofile'")
    assert expect_errstr in str(invalid_args_error.value)
