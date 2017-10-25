import pytest
import data_pipeline.db.factory as dbfactory
import data_pipeline.constants.const as const
from data_pipeline.db.exceptions import UnsupportedDbTypeError

@pytest.mark.parametrize("dbtype, expect_class", [
    (const.ORACLE, "OracleDb"),
    (const.MSSQL, "MssqlDb"),
    (const.POSTGRES, "PostgresDb"),
    (const.GREENPLUM, "GreenplumDb"),
    (const.FILE, "FileDb"),
])
def test_build_oracledb(dbtype, expect_class):
    db = dbfactory.build(dbtype)
    assert type(db).__name__ == expect_class


def test_build_unsupported():
    with pytest.raises(UnsupportedDbTypeError):
        db = dbfactory.build("AnUnsupportedDatabase")
