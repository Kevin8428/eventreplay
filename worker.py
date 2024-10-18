"""
Example implementation - Consume from queue, optionally store messages with temporal partition in S3 so they can be replayed
"""
import os
import logging
import argparse

from eventreplay import eventers
from eventreplay import exceptions

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='worker')
parser.add_argument('--action', action="store", dest='action', default=0)
args = parser.parse_args()

ACTION = args.action
S3_BUCKET = 'event-replay-3jxh'

def main(**kwargs):
    """
    SQS consumer
    """
    eventer = kwargs.get('eventer')
    logger.info('Starting %s consumer', eventer)
    # initialize clients outside of handler, handler just picks one
    client = eventers.client(eventer, **kwargs)
    for _ in client.consume():
        print('msg received')

# ./scripts/worker-init.sh sqs
# ./scripts/worker-init.sh kinesis
if __name__ == "__main__":
    args = None
    match ACTION:
        case 'sqs':
            args = dict(
                action = 'consume',
                eventer = 'sqs',
                queue_name = 'eventreplay',
                persist_messages=True,
                message_store='s3',
                storage_destination=S3_BUCKET,
            )
        case 'kinesis':
            args = dict(
                action = 'consume',
                eventer = 'kinesis',
                stream_name = 'test-1',
            )
        case _:
            raise exceptions.EventerException('action not implemented')

    main(**args)
