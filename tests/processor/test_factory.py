import pytest
import data_pipeline.constants.const as const
import data_pipeline.processor.factory as processor_factory
from data_pipeline.db.exceptions import UnsupportedDbTypeError


def test_build_oracle_processor():
    processor = processor_factory.build(const.ORACLE)
    assert type(processor).__name__ == 'OracleCdcProcessor'

def test_build_mssql_processor():
    processor = processor_factory.build(const.MSSQL)
    assert type(processor).__name__ == 'MssqlCdcProcessor'

def test_build_unsupported():
    with pytest.raises(UnsupportedDbTypeError):
        processor = processor_factory.build("AnUnsupportedDatabase")
