"""
file: producer.py
brief: A simple producer to a kinesis stream.
author: S. V. Paulauskas
date: February 11, 2020
"""
from logging import getLogger
from random import lognormvariate
from time import sleep
from uuid import uuid4

from boto3 import Session

from common.common import read_cli_args, setup_logging


def produce_kinesis_message(cfg):
    setup_logging(cfg['logging'])
    logger = getLogger('producer')

    producer = Session(profile_name='default').client('kinesis')

    if cfg['producer']['message_frequency_hz']:
        rate = 1. / cfg['producer']['message_frequency_hz']
    else:
        rate = None

    while True:
        if not cfg['producer']['num_messages']:
            break
        payload = uuid4().__str__()
        logger.info(payload)
        producer.put_record(StreamName=cfg['stream'], Data=payload.encode(),
                            PartitionKey='a partition key')
        cfg['producer']['num_messages'] -= 1
        if rate:
            sleep(rate)
        else:
            sleep(lognormvariate(1, 1))


if __name__ == '__main__':
    try:
        produce_kinesis_message(read_cli_args())
    except KeyboardInterrupt:
        print("Exiting...")
