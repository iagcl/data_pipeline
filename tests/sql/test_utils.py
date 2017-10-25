import pytest

from data_pipeline.sql.utils import TableName

@pytest.mark.parametrize("schema, tablename, expected_fullname", [
    ('myschema', 'mytable', 'myschema.mytable'),
    (None, 'mytable', 'mytable'),
    ('', 'mytable', 'mytable'),
    ('myschema', None, 'myschema'),
    ('myschema', '', 'myschema'),
    (None, None, ''),
    ('', '', ''),
])
def test_tablename(schema, tablename, expected_fullname):
    tn = TableName(schema, tablename)
    assert str(tn) == expected_fullname
