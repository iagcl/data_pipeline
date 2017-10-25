import pytest
from data_pipeline.db.file_query_results import FileQueryResults
from data_pipeline.stream.file_writer import FileWriter


@pytest.fixture
def setup(tmpdir):
    data_file = tmpdir.mkdir("d").join("data")
    file_writer = FileWriter(str(data_file))
    file_writer.writeln("a0,b0,c0")
    file_writer.writeln("a1,b1,c1")
    file_writer.writeln("a2,b2,c2")
    file_writer.flush()
    file_writer.close()
    yield(str(data_file))


def splitter(line):
    return line.split(',')


def test_fetchone(setup):
    (data_file) = setup
    file_query_results = FileQueryResults(data_file, splitter)
    assert file_query_results.fetchone() == ["a0", "b0", "c0"]


def test_fetchmany(setup):
    (data_file) = setup
    file_query_results = FileQueryResults(data_file, splitter)
    assert file_query_results.fetchmany(arraysize=2) == [["a0", "b0", "c0"],
                                                         ["a1", "b1", "c1"]]


def test_fetchall(setup):
    (data_file) = setup
    file_query_results = FileQueryResults(data_file, splitter)
    assert file_query_results.fetchall() == [["a0", "b0", "c0"],
                                             ["a1", "b1", "c1"],
                                             ["a2", "b2", "c2"]]


def test_foreach(setup):
    (data_file) = setup
    file_query_results = FileQueryResults(data_file, splitter)
    lines = [l for l in file_query_results]
    assert lines == [["a0", "b0", "c0"],
                     ["a1", "b1", "c1"],
                     ["a2", "b2", "c2"]]
