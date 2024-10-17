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
        self.sequences = {}
        self.next_shard_iterator = {}


    def get_sequence_number(self, shard_id):
        """docstring"""
        # TODO: build this feature
        # fetch from s3://kinesis-sequencing/stream-name/shard_id
        return 0
    
    def set_sequence_number(self, shard_id, sequence_number):
        """docstring"""
        self.sequences.setdefault(shard_id, "")
        self.sequences[shard_id] = sequence_number

    def set_next_shard_iterator(self, shard_id, next_shard_iterator):
        """docstring"""
        self.next_shard_iterator.setdefault(shard_id, "")
        self.next_shard_iterator[shard_id] = next_shard_iterator

    def get_next_shard_iterator(self, shard_id):
        """docstring"""
        return self.next_shard_iterator.get(shard_id, None)

    def get_records(self):
        """
        Gets records from the stream. This function is a generator that first gets
        a shard iterator for the stream, then uses the shard iterator to get records
        in batches from the stream. Each batch of records is yielded back to the
        caller until the specified maximum number of records has been retrieved.

        :return: Yields the current batch of retrieved records.
        """
        # TODO
        # - get list of shards
        # - get records from each shard using `get_shard_iterator` and `get_records`
        #   - resharding/child shards are returned only when current shard is exhausted
        #   - DON'T HANDLE resharding (aka splits/merges) or child shards for now
        shards = client.list_shards(
            StreamName=self.name,
        )['Shards']
        # need to read each shard individually
        # need to handle reshard
        #   - when get_records returns info about child shard
        while True:
            for shard in shards:
                shard_id = shard.get('ShardId')
                sequence_number = self.get_sequence_number(shard_id)
                # start from beginning if don't find sequence number

                if sequence_number == 0:
                    sequence_number = shard.get('SequenceNumberRange').get('StartingSequenceNumber')
                else:
                    pass
                    # print('found sequence number: ', sequence_number)

                shard_iter = self.get_next_shard_iterator(shard_id)
                # start from beginning if don't find sequence number
                if shard_iter is None:
                    pass
                else:
                    pass
                    # print('found shard iterator: ', shard_iter)


                print('fetching records - shard: ', shard_id, '; sequence_number: ', sequence_number)
                try:
                    if shard_iter is None:
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
                    # use shard iterator if you want to keep polling from one shard
                    # shard_iter = response["ShardIterator"]
                    response = self.kinesis_client.get_records(
                        ShardIterator=shard_iter, Limit=10
                    )
                    shard_iter = response["NextShardIterator"]
                    self.set_next_shard_iterator(shard_id, shard_iter)
                    _records = response["Records"]
                    if len(_records) == 0:
                        print('no records found - sleeping for 3 seconds')
                        time.sleep(3)
                    else:
                        last_record = _records[len(_records)-1]
                        self.set_sequence_number(shard_id, last_record['SequenceNumber'])
                        logger.info("Got %s records.", len(_records))
                        yield _records
                except Exception as e:
                    logger.exception("Couldn't get records from stream %s: %s", self.name, e)
                    raise


stream = KinesisStream(client, 'test-1')
records = stream.get_records()
for record in records:
    print('found ',len(record), ' records' )
