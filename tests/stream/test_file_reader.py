import bz2
import gzip
import pytest
from data_pipeline.stream.file_reader import FileReader
from data_pipeline.stream.file_writer import FileWriter

def test_read_to(tmpdir, mocker):
    filepath = tmpdir.mkdir("test_filereader_dir").join("input.txt")
    f = filepath.open("a")
    f.write("{'batch_id': '0', 'table_name': 'MYTABLE', 'record_type': 'SOB', 'record_count': 0, 'payload': ''}\n")
    f.write("{'batch_id': '0', 'table_name': 'MYTABLE', 'record_type': 'DATA', 'record_count': 0, 'payload': '1,albert'}\n")
    f.write("{'batch_id': '0', 'table_name': 'MYTABLE', 'record_type': 'EOB', 'record_count': 1, 'payload': ''}\n")
    f.flush()
    f.close()
    mockapplier_config = {}
    mockapplier = mocker.Mock(**mockapplier_config)

    filereader = FileReader(str(filepath))
    filereader.read_to(mockapplier)

    mockapplier.apply.call_count == 3


def test_read_gz(tmpdir):
    tmpfile = tmpdir.mkdir("test_write").join("test_file.dat.gz")
    filename = str(tmpfile)
    with gzip.open(filename, 'w') as f:
        f.write("blah")
    assert_read(filename)


def test_read_bz2(tmpdir):
    tmpfile = tmpdir.mkdir("test_write").join("test_file.dat.bz2")
    filename = str(tmpfile)
    f = bz2.BZ2File(filename, 'w')
    f.write("blah")
    f.close()
    assert_read(filename)


def assert_read(filename):
    tmpfile_handle = FileReader(filename)
    line = tmpfile_handle.readline()
    assert line == "blah"
