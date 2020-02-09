"""
file: pixie_net-consumer.py
brief: A Kafka consumer that processes Pixie16 data. Creates a thread for each partition plus extra.
author: S. V. Paulauskas
date: March 30, 2019
"""
from argparse import ArgumentParser
from logging import getLogger
from logging.config import dictConfig
from statistics import mean
from time import strftime, time, localtime

from confluent_kafka import Consumer, KafkaError
from yaml import safe_load


def main():
    parser = ArgumentParser(description='An Apache Kafka data producer.')
    parser.add_argument('cfg', type=str, help='The configuration file used for producing.')
    args = parser.parse_args()
    with open(args.cfg, 'r') as cfg_file:
        cfg = safe_load(cfg_file)

    dictConfig(cfg['logging'])
    logger = getLogger('consumer')

    consumer = Consumer(
        {'bootstrap.servers': cfg['bootstrap.servers'],
         'group.id': cfg['consumer']['group.id'],
         'default.topic.config': {
             'auto.offset.reset': cfg['consumer']['default.topic.config']['auto.offset.reset']},
         'auto.commit.interval.ms': cfg['consumer']['auto.commit.interval.ms']})
    consumer.subscribe([cfg['topic']])

    timeouts = records = 0
    messages__in_interval = errors_in_interval = idle_time_in_interval = 0
    stats_interval_start_time = time()
    message_processing_times = []

    logger.info("I suppose I ought to eat or drink something or other; but the great question is 'What?'")
    while True:
        msg = consumer.poll(timeout=cfg['consumer']['poll.timeout'])
        if msg is None:
            idle_time_in_interval += cfg['consumer']['poll.timeout']
        elif not msg.error():
            message_processing_start_time = time()
            msg_ts_seconds = msg.timestamp()[1] / 1000
            logger.info(f"{msg.value().decode()} | {strftime(cfg['time_format'], localtime(msg_ts_seconds))}")
            message_processing_times.append(time() - message_processing_start_time)
        elif msg.error().code() != KafkaError._PARTITION_EOF:
            logger.error(msg.error())

        # elapsed_time = time() - stats_interval_start_time
        # if elapsed_time >= cfg['stats_interval_in_seconds']:
        #     msg = {'idle_time': idle_time_in_interval}
        #     if message_processing_times:
        #         msg['min_message_processsing_time'] = min(message_processing_times)
        #         msg['ave_message_processing_time'] = mean(message_processing_times)
        #         msg['max_message_processing_time'] = max(message_processing_times)
        #     print(msg)
        #     stats_interval_start_time = time()
        #
        #     idle_time_in_interval = 0
        #     message_processing_times.clear()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("I do wish I hadn't drunk quite so much!")
