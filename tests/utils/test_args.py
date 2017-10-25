import configargparse
import os
import pytest
import sys
import tests.unittest_utils as unittest_utils

import data_pipeline.constants.const as const
import data_pipeline.utils.utils as utils
import data_pipeline.utils.args as args

from pytest_mock import mocker

@pytest.fixture
def setup(tmpdir):
    try:
        tmppath = str(tmpdir.mkdir("test_dir"))
    except Exception, e:
        # If tmpdir already created
        tmppath = os.path.join(str(tmpdir), "test_dir")

    yield (tmppath,)


@pytest.mark.parametrize("mode, config_file, invalid_arg_name, invalid_arg_value", [
    (const.INITSYNC, "sample_initsync_config.yaml", "--targetcommitpoint", "1000"),
    (const.CDCEXTRACT, "sample_extractor_config.yaml", "--truncate", "True"),
    (const.CDCAPPLY, "sample_applier_config.yaml", "--truncate", "True"),
])
def test_unknown_arg(mode, config_file, invalid_arg_name, invalid_arg_value, mocker, setup):
    (tmppath,) = setup

    mock_sys = mocker.patch("data_pipeline.utils.args.sys")
    mock_sys.argv = ["myprogram",
        "--config", "conf/{}".format(config_file),
        "--workdirectory", tmppath,
        invalid_arg_name, invalid_arg_value,
    ]
    with pytest.raises(SystemExit):
        argv = args.get_program_args(mode)


@pytest.mark.parametrize("mode, config_file", [
    (const.INITSYNC, "sample_initsync_config.yaml"),
    (const.CDCEXTRACT, "sample_extractor_config.yaml"),
    (const.CDCAPPLY, "sample_applier_config.yaml"),
])
def test_workdirectory_append_datetime(mode, config_file, mocker, setup):
    (tmppath,) = setup

    mock_sys = mocker.patch("data_pipeline.utils.args.sys")
    mock_sys.argv = ["myprogram",
        "--config", "conf/{}".format(config_file),
        "--workdirectory", tmppath,
    ]
    mock_time = mocker.patch("data_pipeline.utils.filesystem.time")
    mock_current_datetime = "20170922_163347"
    mock_time.strftime.return_value = mock_current_datetime
    argv = args.get_program_args(mode)

    assert argv.workdirectory == os.path.join(tmppath, mock_current_datetime)


@pytest.mark.parametrize("mode, config_file, auditcommitpoint, expect_error", [
    (const.INITSYNC, "sample_initsync_config.yaml", const.MIN_COMMIT_POINT, False),
    (const.INITSYNC, "sample_initsync_config.yaml", const.MIN_COMMIT_POINT - 1, True),
    (const.CDCAPPLY, "sample_applier_config.yaml", const.MIN_COMMIT_POINT, False),
    (const.CDCAPPLY, "sample_applier_config.yaml", const.MIN_COMMIT_POINT - 1, True),
])
def test_auditcommitpoint_validation(mode, config_file, auditcommitpoint, expect_error, mocker, setup):
    (tmppath,) = setup

    mock_sys = mocker.patch("data_pipeline.utils.args.sys")
    mock_sys.argv = ["myprogram",
        "--config", "conf/{}".format(config_file),
        "--workdirectory", tmppath,
        "--auditcommitpoint", str(auditcommitpoint),
    ]
    if expect_error:
        with pytest.raises(SystemExit) as sysexit:
            argv = args.get_program_args(mode)
    else:
        argv = args.get_program_args(mode)
        assert argv.auditcommitpoint == auditcommitpoint


@pytest.mark.parametrize("mode, config_file, targetcommitpoint, expect_error", [
    # targetcommitpoint not supported for initsync
    (const.INITSYNC, "sample_initsync_config.yaml", const.MIN_COMMIT_POINT, True),
    (const.INITSYNC, "sample_initsync_config.yaml", const.MIN_COMMIT_POINT - 1, True),
    (const.CDCAPPLY, "sample_applier_config.yaml", const.MIN_COMMIT_POINT, False),
    (const.CDCAPPLY, "sample_applier_config.yaml", const.MIN_COMMIT_POINT - 1, True),
])
def test_targetcommitpoint_validation(mode, config_file, targetcommitpoint, expect_error, mocker, setup):
    (tmppath,) = setup

    mock_sys = mocker.patch("data_pipeline.utils.args.sys")
    mock_sys.argv = ["myprogram",
        "--config", "conf/{}".format(config_file),
        "--workdirectory", tmppath,
        "--targetcommitpoint", str(targetcommitpoint),
    ]
    if expect_error:
        with pytest.raises(SystemExit) as sysexit:
            argv = args.get_program_args(mode)
    else:
        argv = args.get_program_args(mode)
        assert argv.targetcommitpoint == targetcommitpoint


@pytest.mark.parametrize("mode, config_file, samplerows, expect_error", [
    (const.INITSYNC, "sample_initsync_config.yaml", 1, False),
    (const.INITSYNC, "sample_initsync_config.yaml", 0, False),
    (const.INITSYNC, "sample_initsync_config.yaml", -1, True),
    (const.CDCEXTRACT, "sample_extractor_config.yaml", 1, False),
    (const.CDCEXTRACT, "sample_extractor_config.yaml", 0, False),
    (const.CDCEXTRACT, "sample_extractor_config.yaml", -1, True),
    (const.CDCAPPLY, "sample_applier_config.yaml", 1, False),
    (const.CDCAPPLY, "sample_applier_config.yaml", 0, False),
    (const.CDCAPPLY, "sample_applier_config.yaml", -1, True),
])
def test_samplerows_validation(mode, config_file, samplerows, expect_error, mocker, setup):
    (tmppath,) = setup

    mock_sys = mocker.patch("data_pipeline.utils.args.sys")
    mock_sys.argv = ["myprogram",
        "--config", "conf/{}".format(config_file),
        "--workdirectory", tmppath,
        "--samplerows", str(samplerows),
    ]
    if expect_error:
        with pytest.raises(SystemExit) as sysexit:
            argv = args.get_program_args(mode)
    else:
        argv = args.get_program_args(mode)
        assert argv.samplerows == samplerows
