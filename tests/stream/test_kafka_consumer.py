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
import pytest
import confluent_kafka
import tests.unittest_utils as unittest_utils
import data_pipeline.constants.const as const

from pytest_mock import mocker
from data_pipeline.stream.kafka_consumer import KafkaConsumer
from confluent_kafka import TopicPartition

poll_calls = 0

@pytest.fixture
def setup(tmpdir, mocker):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv = mocker.Mock(**mockargv_config)
    unittest_utils.setup_logging(mockargv.workdirectory)

    pc_config = {'insert.return_value': None, 'update.return_value': None}
    mock_pc = mocker.Mock(**pc_config)

    mock_applier_config = {
        "apply.side_effect": apply_side_effect,
        "process_control.return_value": mock_pc,
    }
    mock_applier = mocker.Mock(**mock_applier_config)

    yield (mock_applier)


def apply_side_effect(message):
    return message.value()


def test_error_consumer_poll(mocker, setup):
    (mock_applier) = setup

    mock_message_config = { "value.return_value": const.KILLED,
                            "error.return_value": None }
    mock_message = mocker.Mock(**mock_message_config)

    def poll_exception_side_effect(timeout):
        global poll_calls
        poll_calls += 1
        print ("poll_calls = {}".format(poll_calls))
        if poll_calls == 1:
            raise Exception("Poll failed!")
        else:
            return mock_message

    mock_avro_consumer_config = {
        "poll.side_effect": poll_exception_side_effect,
        "committed.return_value": [TopicPartition('topic', 0, 0)]
    }

    mock_avro_consumer = mocker.Mock(**mock_avro_consumer_config)
    mock_avro_consumer_constructor = mocker.patch(
        "data_pipeline.stream.kafka_consumer.AvroConsumer")
    mock_avro_consumer_constructor.return_value = mock_avro_consumer

    kafka_consumer = KafkaConsumer(
        'broker',
        'group',
        'topic',
        'schema_reg_url',
        mock_applier)

    kafka_consumer.consumer_loop()


def test_seek_to_end(mocker, setup):
    global poll_calls
    poll_calls = 0
    
    end_of_queue_offset = 1234
    
    (mock_applier) = setup
    

    mock_message_config = {
        "value.return_value": None,
        "error.return_value": None,
        "offset.return_value": end_of_queue_offset
    }
    mock_message = mocker.Mock(**mock_message_config)

    def poll_message_side_effect(timeout):
        global poll_calls
        poll_calls += 1
        # The first polled message should be the last message in the queue
        if poll_calls == 1:
            return mock_message
        else:
            # Indicates that no more messages are left in the queue
            return None

    mock_avro_consumer_config = {
        "poll.side_effect": poll_message_side_effect,
        "committed.return_value": [TopicPartition('topic', 0, 0)]
    }

    mock_avro_consumer = mocker.Mock(**mock_avro_consumer_config)
    mock_avro_consumer_constructor = mocker.patch(
        "data_pipeline.stream.kafka_consumer.AvroConsumer")
    mock_avro_consumer_constructor.return_value = mock_avro_consumer

    kafka_consumer = KafkaConsumer(
        'broker',
        'group',
        'topic',
        'schema_reg_url',
        mock_applier)

    # Simulate receiving a stats callback with the eof_offset
    kafka_consumer._eof_offset = end_of_queue_offset

    kafka_consumer.seek_to_end(1)

    expected_commit_topic_partition = TopicPartition('topic', 0, end_of_queue_offset)
    mock_avro_consumer.commit.assert_called_once_with(
        async=False,
        offsets=[expected_commit_topic_partition],)
