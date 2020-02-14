"""
file: helper_functions.py
brief: Provides helper functions to use with the Kinesis streams
author: S. V. Paulauskas
date: February 11, 2020
"""
from json import dumps


def list_shard_ids(conn, name):
    """

    :param conn:
    :param name:
    :return:
    """
    return enumerate([shard['ShardId'] for shard in
                      conn.describe_stream(StreamName=name)['StreamDescription']['Shards']])


def pretty_describe_stream(conn, name):
    """
    Calls the describe stream method on the stream and returns the results in a pretty print string.
    :param conn: Connection that we'll use to get the description
    :param name: Name of the stream to describe
    :return: A pretty printed stream description.
    """
    return dumps(conn.connection.describe_stream(StreamName=name), sort_keys=True, indent=2)


def list_consumers(conn, name):
    """
    Lists the consumers of the provided stream
    :param conn: Connection we'll use to get the list
    :param name: The name of the stream
    :return: A list of consumers attached to the provided stream
    """
    return dumps(conn.list_stream_consumers(StreamARN=name), sort_keys=True, indent=2)


def renew_shard_iterator(conn, name, id, type):
    return conn.get_shard_iterator(StreamName=name, ShardId=id, ShardIteratorType=type)[
        'ShardIterator']
