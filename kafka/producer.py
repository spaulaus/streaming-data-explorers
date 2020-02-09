"""
file: producer.py
brief: Simple producer that reads an existing data file into a Kafka Topic
author: S. V. Paulauskas
date: January 27, 2019
"""
from argparse import ArgumentParser
from logging import getLogger
from logging.config import dictConfig
from time import sleep
from uuid import uuid4

from confluent_kafka import Producer
from yaml import safe_load


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def main():
    parser = ArgumentParser(description='An Apache Kafka data producer.')
    parser.add_argument('cfg', type=str, help='The configuration file used for producing.')
    args = parser.parse_args()
    with open(args.cfg, 'r') as cfg_file:
        cfg = safe_load(cfg_file)

    dictConfig(cfg['logging'])
    logger = getLogger('producer')

    p = Producer({'bootstrap.servers': cfg['bootstrap.servers']})

    while True:
        p.poll(0)
        payload = uuid4().__str__()
        logger.info(payload)
        p.produce(cfg['topic'], payload, partition=0, callback=delivery_report)
        p.flush()
        sleep(1./cfg['producer']['message_frequency_hz'])


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting...")
