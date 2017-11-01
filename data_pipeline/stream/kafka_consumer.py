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
# Module:    kafka_consumer
# Purpose:   Utility to read a message from a Kafka topic
#
# Notes:
#
###############################################################################

import json
import time
import sys
import logging
import data_pipeline.constants.const as const

from confluent_kafka import (KafkaError, KafkaException, TopicPartition,
                             OFFSET_END)
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


class KafkaConsumer:

    def __init__(self, broker, group, topic, schema_registry_url, client):
        """ Constructer KafkaConsumer(broker, group, topic)
        :param str broker: The name of Kafka Broker Server and port
        :param str group: The name of Kafka Consumer Group
        :param str topic: The Kafka Topic name
        :param str schema_registry_url: The Kafka Schema Registry Server url
        :param object client: The client instance to pass the kafka message to
        """

        self._logger = logging.getLogger(__name__)
        self._client = client
        self._process_control = client.process_control

        self._broker = broker
        self._group = group
        self._topic = topic
        self._topic_partition = TopicPartition(self._topic, 0)
        self._schema_registry_url = schema_registry_url

        self._consumer = self.build_consumer()
        self._eof_offset = None
        self._committed_offset = None
        self._last_message = None
        self._running = None

    @property
    def last_message(self):
        return self._last_message

    @last_message.setter
    def last_message(self, value):
        if value is not None:
            self._last_message = value

    def consumer_loop(self, timeout=None):
        self._running = True

        self._logger.info("""Polling kafka queue:
            broker={broker}
            topic={topic}
            consumer_group={group}
            schema_registry_url={schema_reg_url}
            current_position={position}
            current_assignment={assignment}
            last_committed={committed}""".format(
            broker=self._broker,
            group=self._group,
            topic=self._topic,
            schema_reg_url=self._schema_registry_url,
            position=self._consumer.position([self._topic_partition]),
            assignment=self._consumer.assignment(),
            committed=self._consumer.committed([self._topic_partition])))

        try:
            while self._running:
                message = self._consumer.poll(timeout)
                self._process_message(message)
        except Exception, e:
            self._logger.exception("Failed to poll kafka queue: {}"
                                   .format(str(e)))
        finally:
            self._consumer.close()

    def _process_message(self, message):
        if message is None:
            return

        if not message.error():
            self._logger.debug("Received message: offset={o} "
                               "key={k} value={v}"
                               .format(o=message.offset(),
                                       k=message.key(),
                                       v=message.value()))

            apply_status = self._client.apply(message)

            if apply_status == const.COMMITTED or apply_status == const.KILLED:
                # commit the last read offset since
                # transaction is complete
                try:
                    self._consumer.commit(async=False)
                except Exception, e:
                    err_message = ("Failed to commit offsets: {}"
                                   .format(str(e)))
                    self._logger.warn(err_message)
                    self._process_control.comment = err_message
                    self._process_control.status = const.WARNING
                    self._process_control.update()

            # Signal consumer loop to stop
            if apply_status == const.KILLED or apply_status == const.ERROR:
                self._running = False

        elif message.error().code() != KafkaError._PARTITION_EOF:
            err_message = ("Error reading topic: {topic}"
                           .format(topic=message.error()))
            self._logger.error(err_message)
            self._process_control.comment = err_message
            self._process_control.status = const.ERROR
            self._process_control.update()

    def build_consumer(self):
        try:
            consumer = AvroConsumer({
                'bootstrap.servers': self._broker,
                'group.id': self._group,
                'schema.registry.url': self._schema_registry_url,
                'enable.auto.commit': False,
                'on_commit': self.log_commit_result,
                'default.topic.config': {'auto.offset.reset':
                                         const.KAFKA_AUTO_OFFSET_RESET},
                'stats_cb': self._stats,
                'error_cb': self._on_error,
                'statistics.interval.ms': const.KAFKA_STATS_INTERVAL_MS
            })
        except Exception:
            err_message = ("Exception in initializing "
                           "Avro Consumer. Broker:{broker} Schema Registry "
                           "URL: {schema_reg_url}"
                           .format(broker=self._broker,
                                   schema_reg_url=self._schema_registry_url))
            self._process_control.comment = err_message
            self._process_control.status = const.ERROR
            self._process_control.update()
            self._logger.exception(err_message)
            raise

        consumer.subscribe([self._topic], on_assign=self._on_assign)
        return consumer

    def seek_to_end(self, timeout):
        message = None

        while True:
            try:
                # Trigger an assignment by polling
                message = self._consumer.poll(float(timeout))

                if message is None:
                    if self.last_message is None:
                        # There are no messages in the queue
                        break
                    elif self._eof_offset != self.last_message.offset():
                        continue

                    self._logger.debug(
                        "[{t}:{cg}] Arrived at end of queue: {o}"
                        .format(t=self._topic,
                                cg=self._group,
                                o=self._eof_offset))

                    # Then commit the assigned offset
                    self._topic_partition.offset = self._eof_offset
                    self._consumer.commit(offsets=[self._topic_partition],
                                          async=False)
                    break
                else:
                    self.last_message = message

            except Exception, e:
                self._logger.exception(
                    "Failed to seek to end in kafka queue: {err}"
                    .format(err=str(e)))
        self._consumer.close()

        if self.last_message is None:
            return OFFSET_END
        return self.last_message.offset()

    def _on_error(self, kafka_error):
        if kafka_error.code() == KafkaError._ALL_BROKERS_DOWN:
            message = "All brokers are down!: {}".format(str(kafka_error))
            self._client.report_error(message)
        else:
            self._logger.warn("Error occurred while consuming from kafka: {}"
                              .format(str(kafka_error)))

    def _stats(self, json_str):
        """Used to populate the self._eof_offset and self._committed_offset
        member variables
        :param str json_str: json string containing consumer statistics data
        """
        statistics = json.loads(json_str)

        topics = statistics.get('topics', None)
        if topics is None:
            return

        topic = topics.get(self._topic, None)
        if topic is None:
            return

        partitions = topic.get('partitions', None)
        if partitions is None:
            return

        partition = partitions.get('0', None)
        if partition is None:
            return

        eof_offset = partition.get('eof_offset', None)
        committed_offset = partition.get('committed_offset', None)
        if eof_offset is None or committed_offset is None:
            return

        self._eof_offset = eof_offset
        self._committed_offset = committed_offset

    def _on_assign(self, consumer, partitions):
        kafka_committed = consumer.committed([self._topic_partition])

        if not kafka_committed:
            if self._client.next_offset_to_read is None:
                self._logger.info(
                    "[{t}:{cg}] No client offset provided. "
                    "Starting at {default_reset} point in kafka queue. "
                    .format(t=self._topic,
                            cg=self._group,
                            default_reset=const.KAFKA_AUTO_OFFSET_RESET))
                return

            else:
                self._logger.info(
                    "[{t}:{cg}] No kafka committed "
                    "offsets found for consumer group. "
                    "Starting at offset {co}."
                    .format(t=self._topic,
                            cg=self._group,
                            co=self._client.next_offset_to_read))

        elif self._client.next_offset_to_read is None:
            self._logger.info(
                "[{t}:{cg}] No client offset provided. "
                "Starting at offset {ko}."
                .format(t=self._topic,
                        cg=self._group,
                        ko=kafka_committed[0].offset))
            return

        elif self._client.next_offset_to_read == OFFSET_END:
            self._logger.info(
                "[{t}:{cg}] Seeking to end of queue."
                .format(t=self._topic,
                        cg=self._group))

        elif kafka_committed[0].offset < self._client.next_offset_to_read:
            self._logger.warn(
                "[{t}:{cg}] kafka ({ko}) < client "
                "({co}) offset. This looks like a recovery scenario. "
                "Starting at client offset {co}."
                .format(t=self._topic,
                        cg=self._group,
                        ko=kafka_committed[0].offset,
                        co=self._client.next_offset_to_read))

        elif kafka_committed[0].offset > self._client.next_offset_to_read:
            self._logger.warn(
                "[{t}:{cg}] kafka ({ko}) > client "
                "({co}) offset. Starting at kafka offset {ko}."
                .format(t=self._topic,
                        cg=self._group,
                        ko=kafka_committed[0].offset,
                        co=self._client.next_offset_to_read))
            return

        else:
            self._logger.info("[{t}:{cg}] kafka == client "
                              "({co}) offset. Starting at offset {co}."
                              .format(t=self._topic,
                                      cg=self._group,
                                      co=self._client.next_offset_to_read))
            return

        consumer.unassign()
        self._topic_partition.offset = self._client.next_offset_to_read
        self._logger.info(
            "[{t}:{cg}] Seeking to offset = {o}"
            .format(t=self._topic,
                    cg=self._group,
                    o=self._topic_partition.offset))

        consumer.assign([self._topic_partition])

    def log_commit_result(self, err, partitions):
        if err is not None:
            self._logger.error(
                "[{group}] Failed to commit offsets: {err}: {partitions}"
                .format(group=self._group, err=err, partitions=partitions))
        else:
            self._logger.info(
                "[{group}] Committed offsets for: {partitions}"
                .format(group=self._group, partitions=partitions))
