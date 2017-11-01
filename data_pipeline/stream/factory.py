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
###############################################################################
# Module:  factory
# Purpose: Builds instances of stream producers and consumers
#
# Notes:
#
###############################################################################

from .kafka_producer import KafkaProducer
from .kafka_consumer import KafkaConsumer
from .file_writer import FileWriter
from .file_reader import FileReader


def build_kafka_producer(argv):
    if not argv.streamhost or not argv.streamchannel:
        return None

    return KafkaProducer(
        argv.streamhost,
        argv.streamchannel,
        argv.streamschemahost,
        argv.streamschemafile)


def build_kafka_consumer(argv, applier):
    if not argv.streamhost or not argv.streamchannel:
        return None

    return KafkaConsumer(
        argv.streamhost,
        argv.streamgroup,
        argv.streamchannel,
        argv.streamschemahost,
        applier)


def build_file_writer(filename):
    return FileWriter(filename)


def build_file_reader(filename):
    return FileReader(filename)
