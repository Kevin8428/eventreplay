"""
SQS eventing consumer and replayer
"""
import os
import logging
import json
from datetime import datetime, timezone

import boto3
from botocore.client import Config

from eventreplay.eventers import base
from eventreplay.storage.s3 import Reader, Writer, File
from eventreplay import exceptions

DELETE_MESSAGES = False # temp for development

SQS_CLIENT = boto3.resource('sqs',
                            region_name='us-west-2',
                            config=Config(connect_timeout=20, retries={'max_attempts': 0}))

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'))
logger = logging.getLogger(__name__)

class SQSMessage():
    """
    Unmarshalled from storage
    """
    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)
        
    @classmethod
    def from_dict(cls, message):
        """
        instantiate class
        """
        return cls(**message)
    
    @classmethod
    def from_binary(cls, b):
        """
        instantiate class
        """
        return cls(**json.loads(b.decode()))
    
    @classmethod
    def from_boto3(cls, message):
        """
        instantiate class
        """
        return cls(**{k: getattr(message, k) for k in dir(message) if isinstance(getattr(message, k), (str, dict)) and k[0]!= '_'})

class SQSConsumer(base.ConsumerClient):
    """
    SQS worker - replay/storage is optional feature
    """
    def __init__(self, queue_name, persist_messages=True, message_store='s3', storage_destination=None):
        self.queue = queue_name
        self.client = self._client()
        self.writer = Writer.from_sqs(storage_destination)
        self.persist_messages = persist_messages
        self.message_store = message_store
        self.logger = logger
        self.visibility_timeout=180
        self.max_number_of_messages=5
        self.wait_time_seconds=5


    def _client(self):
        return SQS_CLIENT.get_queue_by_name(QueueName=self.queue)


    def _persist(self, messages):
        for message in messages:
            sts = message.attributes.get('SentTimestamp')
            if len(sts) == 13:
                sts = round(int(sts)/1000)
            ts = datetime.fromtimestamp(float(sts), tz=timezone.utc).strftime('%Y/%m/%d/%H/%M')
            # TODO: pass entire message, have replayer wrap entire message and include some metadata
            # TODO; make this consumer detect replayed message and unwrap original message
            m = SQSMessage.from_boto3(message)
            file = File(message.message_id, ts, m)
            self.writer.buffer(file)
        self.writer.write()


    def consume(self):
        self.logger.info('Consuming from queue %s', self.queue)
        while True:
            messages = self.client.receive_messages(
                VisibilityTimeout=self.visibility_timeout,
                MaxNumberOfMessages=self.max_number_of_messages,
                WaitTimeSeconds=self.wait_time_seconds,
                AttributeNames=['SentTimestamp'],
            )
            self.logger.info("message count: %d\n", len(messages))
            if self.persist_messages and len(messages) > 0:
                self._persist(messages)
            for message in messages:
                yield message.body
            try:
                entries = [
                    {"Id": str(ind), "ReceiptHandle": msg.receipt_handle}
                    for ind, msg in enumerate(messages)
                ]
                if len(entries) < 1 or not DELETE_MESSAGES: continue
                self.logger.info('delete attempt')
                self.client.delete_messages(
                    Entries=entries
                )
                self.logger.info('delete success')
            except Exception as e:
                self.logger.info('delete failure: %s;', e)
                continue

class SQSReplayer(base.ReplayerClient):
    """
    Replay to given queue for given time range
    """
    def __init__(self, **params):
        # TODO: add dry-run flag
        self.logger = logger
        self.s3_client = boto3.client('s3',region_name='us-west-2')
        self.queue = params.get('queue')
        self.sqs_client = self._client()

    def _client(self):
        return SQS_CLIENT.get_queue_by_name(QueueName=self.queue)

    def replay(self, start, end, bucket):
        """
        Replay intgerface
        """
        reader = Reader(bucket, start, end)
        messages = reader.read()
        self.logger.info('publishing message to queue: %s', self.queue)
        i = 0
        for message in messages:
            i += 1
            msg = SQSMessage.from_binary(message)
            _ = self._copy(msg, self.queue)
        txt = f"Successfully published {i} messages to queue" if i > 0 else "No messages found"
        self.logger.info(txt)
        
    def _copy(self, message, queue):
        try:
            response = self.sqs_client.send_message(
                QueueUrl = queue,
                MessageBody = message.body,
                # MessageAttributes = {} # TODO: set this
                # MessageDeduplicationId = '' # TODO: set this
                # MessageGroupId = '' # TODO: set this
            )
            return response
        except Exception as e:
            self.logger.error('Error replaying to SQS: %s ', e)

        
def client(**kwargs):
    """docstring"""
    # TODO: consider changing this
    action = kwargs.get('action')
    del kwargs['action']
    match action:
        case 'consume':
            return SQSConsumer(**kwargs)
        case 'replay':
            return SQSReplayer(**kwargs)
        case _:
            raise exceptions.EventerException('action not implemented')
        