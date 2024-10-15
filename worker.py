"""
Worker to consume from specified queue.

Worker then publishes message to the provided Cloud Map service.
"""
import os
import logging
import argparse

import boto3
from botocore.client import Config
import requests

from eventreplay.consumers import sqs


logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='worker')
parser.add_argument('--account_id', action="store", dest='account_id', default=0)
args = parser.parse_args()
account_id = args.account_id
logger.info('account_id: ', account_id)

def main():
    """
    SQS consumer
    """
    logger.info('Starting SQS consumer')
    queue_name = 'eventreplay'
    client = sqs.client(queue_name, account_id)
    
logger.info('Starting SQS consumer')

if __name__ == "__main__":
    logger.info('Starting SQS consumer')
    main()
