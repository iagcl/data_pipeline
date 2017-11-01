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
import json
import avro
import requests


class SchemaRegistry:
    def __init__(self, schema_registry_url):
        self.schema_registry_url = schema_registry_url

    def _url(self, path):
        return "http://{}{}".format(self.schema_registry_url, path)

    def get_schema(self, id=None, subject=None, version=None):
        if id:
            return requests.get(self._url('/schemas/ids/{}'.format(id)))
        if subject and version:
            return requests.get(self._url('/subjects/{}/versions/{}'
                                          .format(subject, version)))

    def get_subjects(self):
        return requests.get(self._url('/subjects/'))

    def get_versions(self, subject):
        return requests.get(self._url('/subjects/{}/versions'.format(subject)))

    def create_schema(self, subject, schema):
        return requests.post(self._url('/subjects/{}/versions'
                                       .format(subject)), json=schema)

    def schema_exists(self, subject, schema):
        return requests.post(self._url('/subjects/{}'
                                       .format(subject)), json=schema)

    def schema_compatible(self, subject, version, schema):
        return requests.post(self._url(
            '/compatibility/subjects/{}/versions/{}'
            .format(subject, version)), json=schema)

    def update_config(self, subject=None, config=None):
        if subject:
            return requests.put(self._url('/config/{}'
                                          .format(subject)), json=config)

        return requests.put(self._url('/config'), json=config)

    def get_config(self, subject=None):
        if subject:
            url = self._url('/config/{}'.format(subject))
            print(url)
            return requests.get(url)

        return requests.get(self._url('/config'))
