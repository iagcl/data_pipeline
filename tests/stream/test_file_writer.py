import bz2
import gzip
from data_pipeline.stream.file_writer import FileWriter

def test_write(tmpdir):
    tmpfile = tmpdir.mkdir("test_write").join("test_file.dat")
    filewriter = FileWriter(str(tmpfile))
    filewriter.write("blah")
    filewriter.close()

    tmpfile_handle = open(str(tmpfile), 'r')
    line = tmpfile_handle.readline()
    assert line == "blah"


def test_write_gz(tmpdir):
    tmpfile = tmpdir.mkdir("test_write").join("test_file.dat.gz")
    filename = str(tmpfile)
    filewriter = FileWriter(filename)
    filewriter.write("blah")
    filewriter.close()

    tmpfile_handle = open(filename, 'r')
    line = tmpfile_handle.readline()
    assert line != "blah"

    tmpfile_handle = gzip.open(filename, 'r')
    line = tmpfile_handle.readline()
    assert line == "blah"


def test_write_bz2(tmpdir):
    tmpfile = tmpdir.mkdir("test_write").join("test_file.dat.bz2")
    filename = str(tmpfile)
    filewriter = FileWriter(filename)
    filewriter.write("blah")
    filewriter.close()

    tmpfile_handle = open(filename, 'r')
    line = tmpfile_handle.readline()
    assert line != "blah"

    tmpfile_handle = bz2.BZ2File(filename, 'r')
    line = tmpfile_handle.readline()
    assert line == "blah"
