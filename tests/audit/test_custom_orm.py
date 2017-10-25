import pytest
import data_pipeline.extractor.extractor
import tests.unittest_utils as unittest_utils
import data_pipeline.constants.const as const

import data_pipeline.audit.custom_orm as custom_orm
from data_pipeline.audit.custom_orm import (ProcessControl,
                                            ProcessControlDetail,
                                            SourceSystemProfile)


EXPECTED_PROCESS_CONTROL_INSERT_SQL = """
        INSERT INTO ctl.process_control (
            process_code,
            duration,
            comment,
            status,
            process_starttime,
            process_endtime,
            infolog,
            errorlog,
            profile_name,
            profile_version,
            process_name,
            min_lsn,
            max_lsn,
            filename,
            executor_run_id,
            executor_status,
            source_system_code,
            source_system_type,
            source_region,
            target_system,
            target_region,
            target_system_type,
            object_list,
            total_count
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        ) RETURNING id"""


def get_expected_process_control_values(mocker):
    return ['InitSync', 0, '', 'IN_PROGRESS', mocker.ANY, mocker.ANY, '', '',
            'myprofile', 1, 'InitSync', 123, 456, mocker.ANY, 0, '',
            'myprofile', 'oracle', 'sys', 'myprofile', 'ctl', 'postgres', '', 0]


EXPECTED_PROCESS_CONTROL_DETAIL_INSERT_SQL = """
        INSERT INTO ctl.process_control_detail (
            process_code,
            duration,
            comment,
            status,
            process_starttime,
            process_endtime,
            infolog,
            errorlog,
            run_id,
            object_schema,
            object_name,
            source_row_count,
            insert_row_count,
            update_row_count,
            delete_row_count,
            bad_row_count,
            alter_count,
            create_count,
            delta_starttime,
            delta_endtime,
            delta_startlsn,
            delta_endlsn,
            error_message,
            query_condition
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        ) RETURNING id"""


def get_expected_process_control_detail_insert_values(mocker):
    return ['InitSync', 0, '', 'IN_PROGRESS', mocker.ANY, mocker.ANY, '', '',
            1, '', '', 0, 1, 2, 0, 0, 3, 4, mocker.ANY, mocker.ANY,
            '', '', '', '']


EXPECTED_SOURCE_SYSTEM_PROFILE_INSERT_SQL = """
        INSERT INTO ctl.source_system_profile (
            profile_name,
            version,
            source_system_code,
            source_region,
            target_region,
            object_name,
            object_seq,
            min_lsn,
            max_lsn,
            active_ind,
            history_ind,
            applied_ind,
            delta_ind,
            last_run_id,
            last_process_code,
            last_status,
            last_updated,
            last_applied,
            last_history_update,
            notes
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        ) RETURNING id"""

EXPECTED_SOURCE_SYSTEM_PROFILE_FIELDS = [
    'id',
    'profile_name',
    'version',
    'source_system_code',
    'source_region',
    'target_region',
    'object_name',
    'object_seq',
    'min_lsn',
    'max_lsn',
    'active_ind',
    'history_ind',
    'applied_ind',
    'delta_ind',
    'last_run_id',
    'last_process_code',
    'last_status',
    'last_updated',
    'last_applied',
    'last_history_update',
    'notes'
]


def get_expected_source_system_profile_values(mocker):
    return ['myprofile', 1, 'myprofile', 'sys', 'ctl', '', 0, 123, 456, '',
            '', '', '', 0, '', '', mocker.ANY, None, None, '']


mock_no_query_results = None
mock_single_query_results = None
mock_multi_query_results = None
expected_query_result_values = None
expected_query_result_fields = None


@pytest.fixture()
def setup(tmpdir, mocker):
    global mock_no_query_results
    global mock_single_query_results
    global mock_multi_query_results
    global expected_query_result_values
    global expected_query_result_fields

    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv = mocker.Mock(**mockargv_config)
    unittest_utils.setup_logging(mockargv.workdirectory)

    mockcursor_config = {'fetchone.return_value': [1] }
    mockcursor = mocker.Mock(**mockcursor_config)

    db_config = {'cursor': mockcursor,
                 'execute_query.side_effect': execute_query_se}
    mockdb = mocker.Mock(**db_config)

    mock_dbfactory_build = mocker.patch(
        'data_pipeline.audit.custom_orm.dbfactory.build')
    mock_dbfactory_build.return_value = mockdb

    expected_query_result_values = (
        1, 'myprofile', 1, 'myprofile', 'sys', 'ctl', '', 0, 0, 0, '',
        '', '', '', 0, '', '', '', '', '', '')
    expected_query_result_fields = EXPECTED_SOURCE_SYSTEM_PROFILE_FIELDS

    assert (len(expected_query_result_values) ==
            len(expected_query_result_fields))

    mock_no_query_results_config = {
        'fetchall.return_value': [],
        'get_col_names.return_value': expected_query_result_fields
    }
    mock_no_query_results = mocker.Mock(**mock_no_query_results_config)

    mock_single_query_results_config = {
        'fetchall.return_value': [expected_query_result_values],
        'get_col_names.return_value': expected_query_result_fields
    }
    mock_single_query_results = mocker.Mock(**mock_single_query_results_config)

    mock_multi_query_results_config = {
        'fetchall.return_value': [expected_query_result_values,
                                  expected_query_result_values],
        'get_col_names.return_value': expected_query_result_fields
    }
    mock_multi_query_results = mocker.Mock(**mock_multi_query_results_config)

    yield (mockdb, mockargv)


@pytest.fixture()
def setup_test_constructor(tmpdir, mocker):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv = mocker.Mock(**mockargv_config)
    unittest_utils.setup_logging(mockargv.workdirectory)

    yield (mockargv)

def execute_query_se(query, arraysize, bind_values):
    global mock_no_query_results
    global mock_single_query_results
    global mock_multi_query_results

    select_no_result = """
        SELECT *
        FROM ctl.source_system_profile
        WHERE 1=1
          AND object_name = %s
          AND profile_name = %s
          AND active_ind = %s"""

    select_sql_single_result = """
        SELECT *
        FROM ctl.source_system_profile
        WHERE 1=1
          AND object_name = %s
          AND profile_name = %s"""

    select_sql_multi_result = """
        SELECT *
        FROM ctl.source_system_profile
        WHERE 1=1
          AND profile_name = %s"""

    if query == select_no_result:
        return mock_no_query_results
    elif query == select_sql_single_result:
        return mock_single_query_results
    elif query == select_sql_multi_result:
        return mock_multi_query_results
    else:
        raise Exception("Unexpected query: {}".format(query))


def test_process_control_insert(mocker, setup):
    (mockdb, mockargv) = setup
    pc = ProcessControl(mockargv, const.INITSYNC)
    pc.insert(
        min_lsn=123,
        max_lsn=456
    )
    mockdb.execute.assert_called_with(
        EXPECTED_PROCESS_CONTROL_INSERT_SQL, 
        get_expected_process_control_values(mocker),
        log_sql=mocker.ANY
    )


def test_process_control_update(mocker, setup):
    expected_update_sql = """
        UPDATE ctl.process_control SET
            comment = %s,
            object_name = %s,
            process_starttime = %s,
            process_endtime = %s,
            duration = %s
        WHERE id = %s"""

    (mockdb, mockargv) = setup
    pc = ProcessControl(mockargv, const.INITSYNC)
    pc.insert(
        min_lsn=123,
        max_lsn=456
    )
    pc.update(comment='foo', object_name='bar')
    mockdb.assert_has_calls([
        mocker.call.closed(),
        mocker.call.connect(mocker.ANY),
        mocker.call.execute(EXPECTED_PROCESS_CONTROL_INSERT_SQL,
                            get_expected_process_control_values(mocker),
                            log_sql=mocker.ANY),
        mocker.call.cursor.fetchone(),
        mocker.call.commit(),
        mocker.call.closed(),
        mocker.call.connect(mocker.ANY),
        mocker.call.execute(
            expected_update_sql,
            ['foo', 'bar', mocker.ANY, mocker.ANY, mocker.ANY, 1],
            log_sql=mocker.ANY),
        mocker.call.commit()
    ])


def test_process_control_delete(mocker, setup):
    expected_delete_sql = """
        DELETE
        FROM ctl.process_control
        WHERE id = %s"""

    (mockdb, mockargv) = setup
    pc = ProcessControl(mockargv, const.INITSYNC)
    pc.insert(
        min_lsn=123,
        max_lsn=456
    )
    pc.delete()
    mockdb.assert_has_calls([
        mocker.call.closed(),
        mocker.call.connect(mocker.ANY),
        mocker.call.execute(EXPECTED_PROCESS_CONTROL_INSERT_SQL,
                            get_expected_process_control_values(mocker),
                            log_sql=mocker.ANY),
        mocker.call.cursor.fetchone(),
        mocker.call.commit(),
        mocker.call.closed(),
        mocker.call.connect(mocker.ANY),
        mocker.call.execute(expected_delete_sql, [1], log_sql=mocker.ANY),
        mocker.call.commit()
    ])


def test_process_control_detail_insert(mocker, setup):
    (mockdb, mockargv) = setup
    pc = ProcessControlDetail(mockargv, const.INITSYNC, 1)
    pc.insert(
        insert_row_count=1,
        update_row_count=2,
        alter_count=3,
        create_count=4,
    )
    mockdb.execute.assert_called_with(
        EXPECTED_PROCESS_CONTROL_DETAIL_INSERT_SQL,
        get_expected_process_control_detail_insert_values(mocker),
        log_sql=mocker.ANY)


def test_process_control_detail_update(mocker, setup):
    expected_update_sql = """
        UPDATE ctl.process_control_detail SET
            comment = %s,
            object_name = %s,
            process_starttime = %s,
            process_endtime = %s,
            duration = %s
        WHERE id = %s"""

    (mockdb, mockargv) = setup
    pc = ProcessControlDetail(mockargv, const.INITSYNC, 1)
    pc.insert(
        insert_row_count=1,
        update_row_count=2,
        alter_count=3,
        create_count=4,
    )
    pc.update(comment='foo', object_name='bar')
    mockdb.assert_has_calls([
        mocker.call.closed(),
        mocker.call.connect(mocker.ANY),
        mocker.call.execute(
            EXPECTED_PROCESS_CONTROL_DETAIL_INSERT_SQL,
            get_expected_process_control_detail_insert_values(mocker),
            log_sql=mocker.ANY),
        mocker.call.cursor.fetchone(),
        mocker.call.commit(),
        mocker.call.closed(),
        mocker.call.connect(mocker.ANY),
        mocker.call.execute(expected_update_sql,
            ['foo', 'bar', mocker.ANY, mocker.ANY, mocker.ANY, 1],
            log_sql=mocker.ANY),
        mocker.call.commit()
    ])


def test_process_control_detail_delete(mocker, setup):
    expected_delete_sql = """
        DELETE
        FROM ctl.process_control_detail
        WHERE id = %s"""

    (mockdb, mockargv) = setup
    pc = ProcessControlDetail(mockargv, const.INITSYNC, 1)
    pc.insert(
        insert_row_count=1,
        update_row_count=2,
        alter_count=3,
        create_count=4,
    )
    pc.delete()
    mockdb.assert_has_calls([
        mocker.call.closed(),
        mocker.call.connect(mocker.ANY),
        mocker.call.execute(
            EXPECTED_PROCESS_CONTROL_DETAIL_INSERT_SQL,
            get_expected_process_control_detail_insert_values(mocker),
            log_sql=mocker.ANY),
        mocker.call.cursor.fetchone(),
        mocker.call.commit(),
        mocker.call.closed(),
        mocker.call.connect(mocker.ANY),
        mocker.call.execute(expected_delete_sql, [1], log_sql=mocker.ANY),
        mocker.call.commit()
    ])


def test_source_system_profile_select_none(mocker, setup):
    (mockdb, mockargv) = setup
    ssp = SourceSystemProfile(mockargv)
    success = ssp.select(
        profile_name='foo',
        object_name='bar',
        active_ind='N'
    )
    assert success == False


def test_source_system_profile_select_single(mocker, setup):
    (mockdb, mockargv) = setup
    ssp = SourceSystemProfile(mockargv)
    success = ssp.select(
        profile_name='foo',
        object_name='bar'
    )
    assert success == True


def test_source_system_profile_select_multi(mocker, setup):
    (mockdb, mockargv) = setup
    ssp = SourceSystemProfile(mockargv)
    success = ssp.select(
        profile_name='foo'
    )
    assert success == False


def test_source_system_profile_insert(mocker, setup):
    (mockdb, mockargv) = setup
    ssp = SourceSystemProfile(mockargv)
    ssp.insert(
        min_lsn=123,
        max_lsn=456
    )
    mockdb.execute.assert_called_with(
            EXPECTED_SOURCE_SYSTEM_PROFILE_INSERT_SQL,
            get_expected_source_system_profile_values(mocker),
            log_sql=mocker.ANY)


def test_source_system_profile_update(mocker, setup):
    expected_update_sql = """
        UPDATE ctl.source_system_profile SET
            object_name = %s,
            source_region = %s
        WHERE id = %s"""

    (mockdb, mockargv) = setup
    ssp = SourceSystemProfile(mockargv)
    ssp.insert(
        min_lsn=123,
        max_lsn=456
    )
    ssp.update(source_region='foo', object_name='bar')
    mockdb.assert_has_calls([
        mocker.call.closed(),
        mocker.call.connect(mocker.ANY),
        mocker.call.execute(EXPECTED_SOURCE_SYSTEM_PROFILE_INSERT_SQL,
                            mocker.ANY, log_sql=mocker.ANY),
        mocker.call.cursor.fetchone(),
        mocker.call.commit(),
        mocker.call.closed(),
        mocker.call.connect(mocker.ANY),
        mocker.call.execute(expected_update_sql, ['bar', 'foo', 1], log_sql=mocker.ANY),
        mocker.call.commit()
    ])


def test_source_system_profile_delete(mocker, setup):
    expected_delete_sql = """
        DELETE
        FROM ctl.source_system_profile
        WHERE id = %s"""

    (mockdb, mockargv) = setup
    ssp = SourceSystemProfile(mockargv)
    ssp.insert(
        min_lsn=123,
        max_lsn=456
    )
    ssp.delete()
    mockdb.assert_has_calls([
        mocker.call.closed(),
        mocker.call.connect(mocker.ANY),
        mocker.call.execute(EXPECTED_SOURCE_SYSTEM_PROFILE_INSERT_SQL,
                            mocker.ANY, log_sql=mocker.ANY),
        mocker.call.cursor.fetchone(),
        mocker.call.commit(),
        mocker.call.closed(),
        mocker.call.connect(mocker.ANY),
        mocker.call.execute(expected_delete_sql, [1], log_sql=mocker.ANY),
        mocker.call.commit()
    ])


def test_process_control_constructor(setup_test_constructor):
    (mockargv) = setup_test_constructor
    obj = ProcessControl(mockargv, const.INITSYNC)
    assert obj is not None


def test_process_control_detail_constructor(setup_test_constructor):
    (mockargv) = setup_test_constructor
    fake_process_control_id = 1
    obj = ProcessControlDetail(mockargv, const.INITSYNC, fake_process_control_id)
    assert obj is not None


def test_source_system_profile_constructor(setup_test_constructor):
    (mockargv) = setup_test_constructor
    obj = SourceSystemProfile(mockargv)
    assert obj is not None
