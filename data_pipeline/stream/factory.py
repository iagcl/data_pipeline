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
