"""
file: consumer.py
brief: A Kafka consumer
author: S. V. Paulauskas
date: January 27, 2019
"""
from logging import getLogger
from time import strftime, localtime

from confluent_kafka import Consumer, KafkaError

from common.common import read_cli_args, setup_logging


def consume_kafka_message(cfg):
    """
    Consumes messages from a Kafka topic specified in the configuration
    file. Writes consumed messages into a log file.
    :param cfg: The configuration file for setup.
    :return: Nothing!
    """
    setup_logging(cfg['logging'])
    logger = getLogger('consumer')

    consumer = Consumer(
        {'bootstrap.servers': cfg['bootstrap.servers'],
         'group.id': cfg['consumer']['group.id'],
         'default.topic.config': {
             'auto.offset.reset': cfg['consumer']['default.topic.config']['auto.offset.reset']},
         'auto.commit.interval.ms': cfg['consumer']['auto.commit.interval.ms']})
    consumer.subscribe([cfg['topic']])

    logger.info(
        "I suppose I ought to eat or drink something or other; but the great question is 'What?'")
    while True:
        msg = consumer.poll(timeout=cfg['consumer']['poll.timeout'])
        if msg is None:
            continue
        elif not msg.error():
            msg_ts_seconds = msg.timestamp()[1] / 1000
            logger.info(
                f"{msg.value().decode()} | {strftime(cfg['time_format'], localtime(msg_ts_seconds))}")
        elif msg.error().code() != KafkaError._PARTITION_EOF:
            logger.error(msg.error())


if __name__ == '__main__':
    try:
        consume_kafka_message(read_cli_args())
    except KeyboardInterrupt:
        print("I do wish I hadn't drunk quite so much!")
