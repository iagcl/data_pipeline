import gzip
import pytest

from data_pipeline.db.connection_details import ConnectionDetails
from data_pipeline.db.filedb import FileDb
from data_pipeline.stream.file_writer import FileWriter

TABLENAME_CSV = "CSV"
TABLENAME_GZ = "GZ"
TABLENAME_BZ2 = "BZ2"
TABLENAME_NOEXT = "NOEXT"
TABLENAME_LOWERCASE = "lowercase"


@pytest.fixture
def setup(tmpdir):
    data_dir = tmpdir.mkdir("d")
    data_file_csv = str(data_dir.join("{}.csv".format(TABLENAME_CSV)))
    data_file_gz = str(data_dir.join("{}.csv.gz".format(TABLENAME_GZ)))
    data_file_bz2 = str(data_dir.join("{}.csv.bz2".format(TABLENAME_BZ2)))
    data_file_noext  = str(data_dir.join("{}".format(TABLENAME_NOEXT)))
    data_file_lowercase = str(data_dir.join("{}.csv".format(TABLENAME_LOWERCASE.lower())))

    write_records_to_file(data_file_csv)
    write_records_to_file(data_file_gz)
    write_records_to_file(data_file_bz2)
    write_records_to_file(data_file_noext)
    write_records_to_file(data_file_lowercase)

    filedb = FileDb()
    connection_details = ConnectionDetails(data_dir=str(data_dir))
    filedb.connect(connection_details)

    yield(filedb)


def write_records_to_file(filename):
    file_writer = FileWriter(filename)
    file_writer.writeln("a0,b0,c0")
    file_writer.writeln("a1,b1,c1")
    file_writer.writeln("a2,b2,c2")
    file_writer.flush()
    file_writer.close()


def comma_split(line):
    return line.split(',')


@pytest.mark.parametrize("post_process_func, expected_result", [
    (None, ["a0,b0,c0", "a1,b1,c1", "a2,b2,c2"]),
    (comma_split, [["a0", "b0", "c0"],
                   ["a1", "b1", "c1"],
                   ["a2", "b2", "c2"]]),
])
def test_execute_query(post_process_func, expected_result, mocker, setup):
    (filedb) = setup
    map(lambda t: execute_query_on_table(
                      filedb, t, post_process_func, expected_result), [
        TABLENAME_CSV,
        TABLENAME_GZ,
        TABLENAME_BZ2,
        TABLENAME_NOEXT,
        TABLENAME_LOWERCASE.upper(), # test case insensitivity
    ])


def execute_query_on_table(filedb, tablename, post_process_func, expected_result):
    results = filedb.execute_query(tablename,
                                   10,
                                   post_process_func=post_process_func)
    assert results.fetchall() == expected_result
