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
from setuptools import setup, find_packages
from version import get_version

setup(
    name="data_pipeline",
    version=get_version(),
    description="Data pipelining of data from a source to target DB",
    setup_requires=['pytest-runner'],
    packages=find_packages(),
    tests_require=[
        'pytest',
        'pep8',
        'pytest-cov',
        'pytest-mock >= 1.5.0',
        'pytest-runner >= 2.11.1'
    ],
    install_requires=[
        'apscheduler',
        'avro >= 1.8.1',
        'ConfigArgParse >= 0.12.0',
        'confluent-kafka >= 0.9.4',
        'enum34 >= 1.1.6',
        'flask >= 0.12.2',
        'flask_login >= 0.4.0',
        'flask_wtf >= 0.14.2',
        'keyring >= 10.4.0',
        'keyrings.alt >= 2.2.0',
        'pathos >= 0.2.0',
        'pep8',
        'pytest',
        'pytest-cov',
        'pytest-mock >= 1.5.0',
        'pytest-runner >= 2.11.1',
        'pyyaml >= 3.12',
        'requests >= 2.13.0',
        'sqlalchemy >= 1.1.7',
    ],
)
