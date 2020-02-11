"""
file: common.py
brief: Defines common functions and variables needed for both Kinesis and Kafka
author: S. V. Paulauskas
date: February 11, 2020
"""
from argparse import ArgumentParser
from logging.config import dictConfig
from os.path import dirname, exists
from os import mkdir

from yaml import safe_load


def read_cli_args():
    """
    Reads the CLI arguments and loads the configuration file.
    :return: The configuration dictionary.
    """
    parser = ArgumentParser(description='An Apache Kafka data producer.')
    parser.add_argument('cfg', type=str, help='The configuration file used for setup.')
    args = parser.parse_args()
    with open(args.cfg, 'r') as cfg_file:
        config = safe_load(cfg_file)
    return config


def setup_logging(cfg):
    """
    Sets up the logging and creates the logging directory if it doesn't exist.
    :param cfg: Dictionary configuration for the logging
    :return: nothing!
    """
    if not exists(dirname(cfg['handlers']['consumer']['filename'])):
        mkdir(dirname(cfg['handlers']['consumer']['filename']))
    dictConfig(cfg['logging'])
