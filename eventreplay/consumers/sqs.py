import os
import logging

import boto3
from botocore.client import Config

SQS_CLIENT = boto3.client('sqs',
                          region_name='us-west-2',
                          config=Config(connect_timeout=20, retries={'max_attempts': 0}))

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'))
logger = logging.getLogger(__name__)

class SQSConsumer():
    def __init__(self, queue_name, account_id):
        self.queue = queue_name
        self.client = SQS_CLIENT
        self.account_id = account_id if account_id else os.environ['ACCOUNT_ID']
        self.queue_url = self._queue_url()
        self.logger = logger
        self.visibility_timeout=180,
        self.max_number_of_messages=5,
        self.wait_time_seconds=20,

    def _queue_url(self):
        return self.client.get_queue_url(
            QueueName=self.queue,
            QueueOwnerAWSAccountId=self.account_id
        )

    def consume(self):
        self.logger.info('Consuming from queue %s', self.queue_url)
        while True:
            response = self.client.receive_message(
                QueueUrl=self.queue_url,
                VisibilityTimeout=self.visibility_timeout,
                MaxNumberOfMessages=self.max_number_of_messages,
                WaitTimeSeconds=self.wait_time_seconds,
            )
            for message in response.get('Messages', []):
                receipt = self.logger.info('received message: %s;', message['ReceiptHandle'])
                self.logger.info('received message: %s;', receipt)
                try:
                    # is body object or string?
                    body = message.get('Body',{})
                    self.logger.info('message: %s; body: %s', message, body)
                except Exception as e:
                    self.logger.error('Error processing message: %s ', e)
                    continue
                self.logger.info('deleting message %s', message)
                response = self.client.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=receipt
                    )
                self.logger.info('deleting message: %s;', receipt)
        

def client(**kwargs):
    return SQSConsumer(**kwargs)