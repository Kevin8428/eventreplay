"""
Kinesis eventing consumer and replayer
"""
import os
import logging

import boto3

KINESIS_CLIENT = boto3.client('kinesis')
S3_CLIENT = boto3.client('s3',
                         region_name='us-west-2')

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'))
logger = logging.getLogger(__name__)

class KinesisConsumer():
    "docstring"
    def consume(self):
        "docstring"
        pass

class KinesisReplayer():
    "docstring"
    def __init__(self, **params):
        self.logger = logger
        self.s3_client = S3_CLIENT
        self.sqs_client = KINESIS_CLIENT

    def replay(self):
        "docstring"
        pass

def client(**kwargs):
    """docstring"""
    action = kwargs.get('action','consume')
    match action:
        case 'consume':
            return KinesisConsumer(**kwargs)
        case 'replay':
            return KinesisReplayer(**kwargs)
        case _:
            raise exceptions.EventerException('action not implemented')
        