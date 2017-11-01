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
# Module:    KafkaProducer
# Purpose:   Utility to write a message to a Kafka topic
#
# Notes:     Example:
#            myproducer = KafkaProduce(broker, topic,
#                                      schema_registry_url, schema_filename)
#            if myproducer.send(message):
#               print "success"
#
###############################################################################

import logging
import data_pipeline.constants.const as const

from .stream_writer import StreamWriter
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


class KafkaProducer(StreamWriter):
    """
    Class KafkaProducer is initiated from Extractor
    """
    def __init__(self, broker, topic,
                 schema_registry_url, value_schema_filename):
        """Construct a KafkaProducer
        :param str broker: The name of Kafka Broker Server and port
        :param str topic: The Kafka Topic name
        :param str schema_registry_url: The Kafka Schema Registry Server url
        :param str value_schema: The Kafka Avro Schema file name
        :return: Returns True if successful otherwise False
        :rtype: Boolean
        """

        self.broker = broker
        self.topic = topic
        self.schema_registry_url = schema_registry_url
        self.value_schema_filename = value_schema_filename
        self._logger = logging.getLogger(__name__)
        self.config = {
            'bootstrap.servers': self.broker,
            'schema.registry.url': self.schema_registry_url,
            'queue.buffering.max.ms': const.KAFKA_MAX_BUFFERED_MS,
            'queue.buffering.max.messages': const.KAFKA_MAX_BUFFERED_MSG,
        }

        try:
            # read the avro schema file
            self.value_schema = avro.load(self.value_schema_filename)
        except Exception:
            self._logger.exception(
                "Extractor: exception in opening the avro schema file: {file} "
                .format(file=self.value_schema_filename))
            raise
        try:
            self._producer = AvroProducer(
                self.config,
                default_value_schema=self.value_schema)

            self._logger.info("Opened stream output for writing: {}/{}, "
                              "schemaregistry: {}, schemafile: {}"
                              .format(
                                  self.broker,
                                  self.topic,
                                  self.schema_registry_url,
                                  self.value_schema_filename))

        except Exception:
            self._logger.exception(
                "Extractor: exception in initializing Avro Producer. "
                "Broker:{broker} Schema Registry URL: {schema_reg_url} "
                "Schema File: {schema_file}"
                .format(
                    broker=self.broker,
                    schema_reg_url=self.schema_registry_url,
                    schema_file=self.value_schema_filename))
            raise

    def write(self, message):
        """ Sends a message to the Kafka Topic """

        try:
            # send message to kafka topic
            self._producer.produce(topic=self.topic, value=message)

            # As per the recommendation:
            # https://github.com/confluentinc/confluent-kafka-python/issues/16
            self._producer.poll(0)
        except Exception:
            self._logger.exception(
                "Extractor: exception in writing into Kafka topic: {topic}"
                .format(topic=self.topic))
            return False

        self._logger.debug("Message sent to topic: {topic}"
                           .format(topic=self.topic))
        return True

    def flush(self):
        self._logger.info("Flushing kafka producer queue")
        self._producer.flush()
