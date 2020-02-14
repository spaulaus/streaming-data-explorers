"""
file: consumer.py
brief: A simple consumer from a Kinesis Stream, we use one consumer for all shards.
author: S. V. Paulauskas
date: February 11, 2020
"""
from logging import getLogger
from time import sleep

from boto3 import Session

from common.common import read_cli_args, setup_logging
from kinesis.helper_functions import list_shard_ids, renew_shard_iterator


def consume_kinesis_message(cfg):
    """
    Consumes messages from a Kafka topic specified in the configuration
    file. Writes consumed messages into a log file.
    :param cfg: The configuration file for setup.
    :return: Nothing!
    """
    setup_logging(cfg['logging'])
    logger = getLogger('consumer')

    consumer = Session(profile_name='default').client('kinesis')

    shards = []
    for i, shard_name in list_shard_ids(consumer, cfg['stream']):
        shards.append({
            'id': i,
            'iterator': renew_shard_iterator(consumer, cfg['stream'], shard_name,
                                             cfg['consumer']['shard_iterator_type']),
        })

    while True:
        for shard in shards:
            payload = consumer.get_records(ShardIterator=shard['iterator'])
            for record in payload['Records']:
                logger.info(f"{cfg['arn']}|{shard['id']}|"
                            f"{record['ApproximateArrivalTimestamp']}|{record['Data'].decode()}")
            shard['iterator'] = payload.get('NextShardIterator')
        # NOTE: We have to put an artificial wait here to ensure that we don't run afoul of AWS
        #       rate limitations.
        sleep(1)


if __name__ == '__main__':
    try:
        consume_kinesis_message(read_cli_args())
    except KeyboardInterrupt:
        print("I do wish I hadn't drunk quite so much!")
