"""
Example implementation - Consume from queue, optionally store messages with temporal partition in S3 so they can be replayed
"""
import os
import logging
import argparse

from eventreplay.consumers import sqs

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='worker')
parser.add_argument('--account_id', action="store", dest='account_id', default=0)
args = parser.parse_args()
account_id = args.account_id
S3_BUCKET = 'event-replay-3jxh'

def main():
    """
    SQS consumer
    """
    logger.info('Starting SQS consumer')
    queue_name = 'eventreplay'
    # TODO: remove need for account id
    client = sqs.client(queue_name=queue_name,
                        account_id=account_id,
                        persist_messages=True,
                        message_store='s3',
                        storage_destination=S3_BUCKET)
    
    for _ in client.consume():
        print('msg received')
    

if __name__ == "__main__":
    main()
