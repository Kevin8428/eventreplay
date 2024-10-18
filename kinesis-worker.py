import os
import time
import logging

import boto3

client = boto3.client('kinesis')

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'))
logger = logging.getLogger(__name__)

class KinesisStream:
    """Encapsulates a Kinesis stream."""

    def __init__(self, kinesis_client, stream_name):
        """
        :param kinesis_client: A Boto3 Kinesis client.
        """
        self.kinesis_client = kinesis_client
        self.name = stream_name
        self.stream_exists_waiter = kinesis_client.get_waiter("stream_exists")
        self.next_sequence_number = {}
        self.next_shard_iterator = {}


    def get_sequence_number(self, shard_id):
        """docstring"""
        return self.next_sequence_number.get(shard_id, None)
    
    def set_sequence_number(self, shard_id, sequence_number):
        """
        Checkpoint shard position. For this to be of any use, can't store in memory.
        Store in S3, EFS, Dynamo, etc.
        """
        self.next_sequence_number.setdefault(shard_id, "")
        self.next_sequence_number[shard_id] = sequence_number

    def get_next_shard_iterator(self, shard_id):
        """docstring"""
        return self.next_shard_iterator.get(shard_id, None)
    
    def set_next_shard_iterator(self, shard_id, next_shard_iterator):
        """docstring"""
        self.next_shard_iterator.setdefault(shard_id, "")
        self.next_shard_iterator[shard_id] = next_shard_iterator

    def get_records(self):
        """
        Gets records from the stream. This function is a generator.
        """
        shards = client.list_shards(
            StreamName=self.name,
        )['Shards']

        while True:
            for shard in shards:
                shard_id = shard.get('ShardId')
                sequence_number = self.get_sequence_number(shard_id)
                if sequence_number is None: # start from beginning
                    sequence_number = shard.get('SequenceNumberRange').get('StartingSequenceNumber')
                shard_iter = self.get_next_shard_iterator(shard_id)
                if shard_iter is None:
                    try:
                        # use sequence number to get shard iterator, since we don't have one saved
                        shard_iter = self.kinesis_client.get_shard_iterator(
                                StreamName=self.name,
                                ShardId=shard_id,
                                ShardIteratorType='AT_SEQUENCE_NUMBER',
                                StartingSequenceNumber=sequence_number, # can set this to shard.SequenceNumberRange.StartingSequenceNumber
                                # ShardIteratorType='AT_SEQUENCE_NUMBER'|'AFTER_SEQUENCE_NUMBER'|'TRIM_HORIZON'|'LATEST'|'AT_TIMESTAMP',
                                # ShardIteratorType="AFTER_SEQUENCE_NUMBER", # use this when you have last latest message read by app
                                # ShardIteratorType="LATEST", # this will just read next incoming message
                                # ShardIteratorType="TRIM_HORIZON", # this will start from oldest record in shard
                            )["ShardIterator"]
                    except Exception as e:
                        logger.exception("Error getting shard iterator %s: %s", self.name, e)
                        raise
                print('fetching records - shard: ', shard_id, '; sequence_number: ', sequence_number)
                try:
                    response = self.kinesis_client.get_records(
                        ShardIterator=shard_iter, Limit=10
                    )
                    shard_iter = response["NextShardIterator"]
                    self.set_next_shard_iterator(shard_id, shard_iter)
                    _records = response["Records"]
                    if _records:
                        yield _records
                    time.sleep(3)
                except Exception as e:
                    logger.exception("Couldn't get records from stream %s: %s", self.name, e)
                    raise


stream = KinesisStream(client, 'test-1')
records = stream.get_records()
for record in records:
    print('found record: ', record)
