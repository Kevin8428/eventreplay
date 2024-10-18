"""
Kinesis eventing consumer and replayer
"""
import os
import time
import logging

import boto3

from eventreplay.eventers import base
from eventreplay import exceptions

KINESIS_CLIENT = boto3.client('kinesis')
S3_CLIENT = boto3.client('s3',
                         region_name='us-west-2')

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'))
logger = logging.getLogger(__name__)

class KinesisConsumer(base.ConsumerClient):
    """Encapsulates a Kinesis stream."""

    def __init__(self, stream_name, persist_messages=True, message_store='s3', storage_destination=None):
        """docstring"""
        # TODO: call base.__init__
        self.logger = logger # remove once you call base.__init__
        self.kinesis_client = KINESIS_CLIENT
        self.s3_client = S3_CLIENT
        self.name = stream_name
        self.stream_exists_waiter = self.kinesis_client.get_waiter("stream_exists")
        self.next_sequence_number = {}
        self.next_shard_iterator = {}
        self.persist_messages = persist_messages
        self.message_store = message_store
        self.storage_destination = storage_destination


    def get_sequence_number(self, shard_id):
        """need to persist somwhere like s3 or EFS"""
        return self.next_sequence_number.get(shard_id, None)
    
    def set_sequence_number(self, shard_id, sequence_number):
        """
        need to persist somwhere like s3 or EFS
        Checkpoint shard position. For this to be of any use, can't store in memory.
        """
        self.next_sequence_number.setdefault(shard_id, "")
        self.next_sequence_number[shard_id] = sequence_number

    def get_next_shard_iterator(self, shard_id):
        """need to persist somwhere like s3 or EFS"""
        return self.next_shard_iterator.get(shard_id, None)
    
    def set_next_shard_iterator(self, shard_id, next_shard_iterator):
        """need to persist somwhere like s3 or EFS"""
        self.next_shard_iterator.setdefault(shard_id, "")
        self.next_shard_iterator[shard_id] = next_shard_iterator

    def consume(self):
        """
        Gets records from the stream. This function is a generator.
        """
        shards = self.kinesis_client.list_shards(
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
                        shard_iter = self.kinesis_client.get_shard_iterator(
                                StreamName=self.name,
                                ShardId=shard_id,
                                ShardIteratorType='AT_SEQUENCE_NUMBER', # use this when you have last latest message read by app (or use starting sequence number)
                                StartingSequenceNumber=sequence_number,
                                # ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                                # ShardIteratorType="LATEST", # this will just read next incoming message to the shard
                                # ShardIteratorType="TRIM_HORIZON", # this will start from oldest record in shard
                            )["ShardIterator"]
                    except Exception as e:
                        raise exceptions.EventerConsumerException(f'Error getting shard iterator {self.name}: {e}')
                self.logger.info('fetching records - shard: %s; sequence_number: %s', shard_id, sequence_number)
                try:
                    response = self.kinesis_client.get_records(
                        ShardIterator=shard_iter, Limit=10
                    )
                    shard_iter = response['NextShardIterator']
                    self.set_next_shard_iterator(shard_id, shard_iter)
                    _records = response['Records']
                    if self.persist_messages:
                        # self.s3_client.put_object(Body=)
                        print('TODO: persist messages')
                    if _records:
                        # optionally store these messages in S3 for replay
                        yield _records
                    time.sleep(3)
                except Exception as e:
                    raise exceptions.EventerConsumerException(f'Couldn\'t get records from stream {self.name}: {e}')

class KinesisReplayer(base.ReplayerClient):
    "docstring"
    def __init__(self, **params):
        # TODO: call base.__init__
        self.logger = logger # remove once you call base.__init__
        self.s3_client = S3_CLIENT
        self.sqs_client = KINESIS_CLIENT

    def replay(self, start, end, bucket):
        "docstring"
        self.logger.info('running replay')

def client(**kwargs):
    """docstring"""
    # TODO: consider changing this
    action = kwargs.get('action')
    del kwargs['action']
    match action:
        case 'consume':
            return KinesisConsumer(**kwargs)
        case 'replay':
            return KinesisReplayer(**kwargs)
        case _:
            raise exceptions.EventerException('action not implemented')
        