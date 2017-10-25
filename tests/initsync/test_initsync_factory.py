import pytest
import data_pipeline.constants.const as const
import tests.unittest_utils as unittest_utils

from data_pipeline.initsync.factory import build

@pytest.fixture
def setup(mocker, tmpdir):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    yield (mockargv_config)


@pytest.mark.parametrize("dbtype", [
    (const.ORACLE),
    (const.POSTGRES),
    (const.GREENPLUM),
    (const.MSSQL),
])
def test_build(dbtype, mocker, setup):
    (mockargv_config) = setup
    mockargv = mocker.Mock(**mockargv_config)

    db = build(dbtype, mockargv, None)
    assert type(db).__name__.lower() == "{}db".format(dbtype)
