"""
SQS eventing consumer and replayer
"""
import os
import logging
import json
from datetime import datetime, timezone

import boto3
import pytz
from botocore.client import Config

from eventreplay.consumers import base
from eventreplay.s3 import reader as s3Reader

DELETE_MESSAGES = False # temp for development
S3_CLIENT = boto3.client('s3',
                         region_name='us-west-2')
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

class SQSReplayer():
    """
    Replay to given queue for given time range
    """
    def __init__(self, **params):
        self.logger = logger
        self.s3_client = S3_CLIENT
        self.queue = params.get('queue')
        self.sqs_client = self._client()

    def _client(self):
        return SQS_CLIENT.get_queue_by_name(QueueName=self.queue)

    def replay(self, start, end, bucket):
        """
        Replay intgerface
        """
        reader = s3Reader.Reader(bucket, start, end)
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



class SQSConsumer(base.Client):
    def __init__(self, queue_name, account_id, persist_messages=True, message_store='s3', storage_destination=None):
        self.queue = queue_name
        self.account_id = account_id if account_id else os.environ['ACCOUNT_ID']
        self.client = self._client()
        self.s3_client = S3_CLIENT
        self.persist_messages = persist_messages
        self.message_store = message_store
        self.storage_destination = storage_destination
        self.logger = logger
        self.visibility_timeout=180
        self.max_number_of_messages=5
        self.wait_time_seconds=5

    def _client(self):
        return SQS_CLIENT.get_queue_by_name(QueueName=self.queue)

    def _persist_message(self, messages):
        prefixes = {}
        for message in messages:
            body = message.body
            sts = message.attributes.get('SentTimestamp')
            if len(sts) == 13:
                sts = round(int(sts)/1000)
            ts = datetime.fromtimestamp(float(sts), tz=timezone.utc).strftime('%Y/%m/%d/%H/%M')
            ts_est = datetime.fromtimestamp(float(sts), tz=pytz.timezone("America/New_York")).strftime('%Y/%m/%d %H:%M:%S')
            # TODO: pass entire message, have replayer wrap entire message and include some metadata
            # TODO; make this consumer detect replayed message and unwrap original message
            prefixes.setdefault(ts, []).append({
                'message_id': message.message_id,
                'send_timestamp_str_est': ts_est,
                'queue_url': message.queue_url,
                'body': message.body,
                'attributes': message.attributes,
                'receipt_handle': message.receipt_handle,
                'md5_of_body': message.md5_of_body,
            })
        for prefix, messages in prefixes.items():
            for message in messages:
                try:
                    body = json.dumps(message, indent=2).encode('utf-8')
                    bucket = self.storage_destination
                    key = f"{prefix}/{message.get('message_id')}"
                    self.s3_client.put_object(Body=body, Bucket=bucket, Key=key)
                    self.logger.info('saved to s3: s3://%s/%s ', bucket, key)
                except Exception as e:
                    self.logger.error('Error saving to s3: %s ', e)

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
            for message in messages:
                self.logger.info('received message: %s;', message)
                yield message.body
            if self.persist_messages and len(messages) > 0:
                    self._persist_message(messages)
            try:
                entries = [
                    {"Id": str(ind), "ReceiptHandle": msg.receipt_handle}
                    for ind, msg in enumerate(messages)
                ]
                if len(entries) < 1: continue
                if not DELETE_MESSAGES: continue
                self.logger.info('delete attempt')
                self.client.delete_messages(
                    Entries=entries
                )
                self.logger.info('delete success')
            except Exception as e:
                self.logger.info('delete failure: %s;', e)
                continue
        
# TODO: change package from `consumers` to `eventers`
def client(**kwargs):
    # TODO: move this logic - this way you are needing to fetch `action` in 
    # every implementation of `consumers`
    action = kwargs.get('action','consume')
    # TODO: make actions constants outside this package
    match action:
        case 'consume':
            return SQSConsumer(**kwargs)
        case 'replay':
            return SQSReplayer(**kwargs)
        case _:
            raise 'action not implemented'
        