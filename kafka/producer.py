"""
file: producer.py
brief: Simple producer that reads an existing data file into a Kafka Topic
author: S. V. Paulauskas
date: January 27, 2019
"""
from logging import getLogger
from random import lognormvariate
from time import sleep
from uuid import uuid4

from confluent_kafka import Producer

from common.common import read_cli_args, setup_logging


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def produce_kafka_messages(cfg):
    """
    Produces kafka messages using the provided configuration file.
    :param cfg: The configuration file containing setup information.
    :return: Nothing!
    """
    setup_logging(cfg['logging'])
    logger = getLogger('producer')

    if cfg['producer']['message_frequency_hz']:
        rate = 1. / cfg['producer']['message_frequency_hz']
    else:
        rate = None

    p = Producer({'bootstrap.servers': cfg['bootstrap.servers']})

    while True:
        if not cfg['producer']['num_messages']:
            break
        p.poll(0)
        payload = uuid4().__str__()
        logger.info(payload)
        p.produce(cfg['topic'], payload, partition=0, callback=delivery_report)
        p.flush()
        cfg['producer']['num_messages'] -= 1
        if rate:
            sleep(rate)
        else:
            sleep(lognormvariate(1, 1))


if __name__ == '__main__':
    try:
        produce_kafka_messages(read_cli_args())
    except KeyboardInterrupt:
        print("Exiting...")
