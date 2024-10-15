import os
import logging
import json
from datetime import datetime, timezone

import boto3
from botocore.client import Config

SQS_CLIENT = boto3.resource('sqs',
                            region_name='us-west-2',
                            config=Config(connect_timeout=20, retries={'max_attempts': 0}))

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'))
logger = logging.getLogger(__name__)


class SQSConsumer():
    def __init__(self, queue_name, account_id, persist_messages=True, message_store='s3', storage_destination=None):
        self.queue = queue_name
        self.account_id = account_id if account_id else os.environ['ACCOUNT_ID']
        # self.queue_url = self._queue_url()
        self.client = self._client()
        self.s3_client = self._s3_client()
        self.persist_messages = persist_messages
        self.message_store = message_store
        self.storage_destination = storage_destination
        self.logger = logger
        self.visibility_timeout=180
        self.max_number_of_messages=5
        self.wait_time_seconds=5

    def _s3_client(self):
        return boto3.client('s3', region_name='us-west-2')

    def _client(self):
        return SQS_CLIENT.get_queue_by_name(QueueName=self.queue)
    
    # def _queue_url(self):
    #     return boto3.client('sqs',region_name='us-west-2').get_queue_url(
    #         QueueName=self.queue,
    #         QueueOwnerAWSAccountId=self.account_id
    #     )['QueueUrl']


    def _persist_message(self, messages):
        prefixes = {}
        for message in messages:
            body = message.body
            sts = message.attributes.get('SentTimestamp')
            if len(sts) == 13:
                sts = round(int(sts)/1000)
            ts = datetime.fromtimestamp(float(sts), tz=timezone.utc).strftime('%Y/%m/%d/%H/%M')
            prefixes.setdefault(ts, []).append({
                'message_id': message.message_id,
                'queue_url': message.queue_url,
                'body': message.body,
                'attributes': message.attributes,
                'receipt_handle': message.receipt_handle,
                'md5_of_body': message.md5_of_body,
            })
        for prefix in prefixes:
            for message in prefix:
                response = self.s3_client.put_object(body=message, bucket=self.storage_destination, Key=prefix)

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
            # for message in messages:
            #     self.logger.info('received message: %s;', message)
            #     try:
            #         body = message.body
            #         receipt = message.receipt_handle
            #         self.logger.info('message body: %s;', body)
            #         # self.logger.info('message receipt: %s', receipt)
            #     except Exception as e:
            #         self.logger.error('Error processing message: %s ', e)
            #         continue
            #     yield body
            if self.persist_messages and len(messages) > 0:
                    self._persist_message(messages)
            # try:
            #     entries = [
            #         {"Id": str(ind), "ReceiptHandle": msg.receipt_handle}
            #         for ind, msg in enumerate(messages)
            #     ]
            #     if len(entries) < 1: continue
            #     self.logger.info('delete attempt')
            #     self.client.delete_messages(
            #         Entries=entries
            #     )
            #     self.logger.info('delete success')
            # except Exception as e:
            #     self.logger.info('delete failure: %s;', e)
            #     continue
        

def client(**kwargs):
    return SQSConsumer(**kwargs)