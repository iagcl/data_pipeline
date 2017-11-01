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
# Source: https://github.com/Changaco/version.py

from os.path import dirname, isdir, join, abspath
import re
import inspect
from subprocess import CalledProcessError, check_output, Popen


PREFIX = 'v'

tag_re = re.compile(r'\btag: %s([0-9][^,]*)\b' % PREFIX)
version_re = re.compile('^Version: (.+)$', re.M)


def get_version():
    if not is_git_repo():
        return "unknown.version"

    # Return the version if it has been injected into the file by git-archive
    version = tag_re.search('$Format:%D$')
    if version:
        return version.group(1)

    filepath = abspath(inspect.getfile(inspect.currentframe()))
    execute_dir = dirname(filepath)

    if filepath:
        # Get the version using "git describe".
        cmd = 'git describe --tags --match %s[0-9]* --dirty' % PREFIX
        try:
            version = check_output(cmd.split(), cwd=execute_dir).decode().strip()[len(PREFIX):]
        except CalledProcessError:
            cmd = 'git describe --always'
            version = check_output(cmd.split(), cwd=execute_dir).decode().strip()

        # PEP 440 compatibility
        if '-' in version:
            dirty = version.endswith('-dirty')
            version = '.post'.join(version.split('-')[:2])
            if dirty:
                version = '{version}*'.format(version=version)

    return version


def is_git_repo():
    try:
	# pipe output to /dev/null for silence
	null = open("/dev/null", "w")
	Popen(["git", "status"], stdout=null, stderr=null)
	null.close()
    except OSError:
	return False

    return True
