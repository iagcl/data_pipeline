# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# 
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
