"""
file: kinesis_to_kafka.py
brief: Reads data from a Kinesis Stream into a Kafka Stream.
author: S. V. Paulauskas
date: November 30, 2018
"""
from logging import basicConfig, info, INFO, error, getLogger
from time import sleep

from boto3 import client
from botocore import exceptions
from confluent_kafka import Producer, KafkaException

from kinesis.helper_functions import list_shard_ids, get_aws_secret_access_key
from common.common import environment_validation

basicConfig(format='%(asctime)s | %(message)s', level=INFO, filename=CONSUMER_CONF['log'])


def renew_shard_iterator(conn, name, id, type):
    return conn.get_shard_iterator(StreamName=name, ShardId=id, ShardIteratorType=type)[
        'ShardIterator']


try:
    environment_validation()
    info("I suppose I ought to eat or drink something or other; but the great question is 'What?'")
    CONSUMER = client(service_name='kinesis',
                      region_name=COMMON_CONF['region'],
                      aws_access_key_id=CONSUMER_CONF['aws_access_key_id'],
                      aws_secret_access_key=get_aws_secret_access_key())
    PRODUCER = Producer({'streams.producer.default.stream': KAFKA_CONF['stream']})

    shards = []
    for i, shard_name in list_shard_ids(CONSUMER, COMMON_CONF['name']):
        shards.append({
            'id': i,
            'iterator': renew_shard_iterator(CONSUMER, COMMON_CONF['name'], shard_name,
                                             CONSUMER_CONF['shard_iterator_type']),
        })

    while True:
        info("Starting new consumption cycle.")
        for shard in shards:
            payload = CONSUMER.get_records(ShardIterator=shard['iterator'])
            for record in payload['Records']:
                info("%s | %s" % (shard['id'], record['Data'].decode()))
                PRODUCER.produce(KAFKA_CONF['topic'], record['Data'], partition=shard['id'])
            shard['iterator'] = payload.get('NextShardIterator')
            PRODUCER.flush()
        getLogger().handlers[0].flush()
        # NOTE: We have to put an artificial wait here to ensure that we don't run afoul of AWS
        #       rate limitations.
        sleep(1)
except exceptions.ClientError as ex:
    error("Received Client Error from Kinesis!\n", ex)
except KafkaException as err:
    error(err)
    info("This was not an encouraging opening for a conversation.")
except EnvironmentError as environment_error:
    error(environment_error)
    info("This was not an encouraging opening for a conversation.")
except KeyboardInterrupt:
    pass
finally:
    info("I do wish I hadn’t drunk quite so much!")
