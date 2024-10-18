"""
Example implementation - Replay messages to designated eventing service
"""
import os
import logging
import argparse

from eventreplay import eventers
from eventreplay import exceptions

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='replayer')
parser.add_argument('--action', action="store", dest='action', default=0)
args = parser.parse_args()

ACTION = args.action
S3_BUCKET = 'event-replay-3jxh'
QUEUE_NAME = 'eventreplay'

def main(**kwargs):
    """
    SQS eventer
    """
    start = kwargs.get('start')
    end = kwargs.get('end')
    eventer = kwargs.get('eventer')
    logger.info('Starting eventer %s\n', eventer)

    client = eventers.client(eventer, **kwargs)
    client.replay(start, end, S3_BUCKET)
    

if __name__ == "__main__":
    args = None
    start = "2024/10/15/17/00" # yyyy/mm/dd/hh/mm - must be UTC for now
    end = "2024/10/15/19/30"
    match ACTION:
        case 'sqs':
            args = dict(
                action = 'replay',
                eventer = 'sqs',
                start = start,
                end = end,
                queue = "fake-service",
            )
        case 'kinesis':
            args = dict(
                action = 'replay',
                eventer = 'kinesis',
                start = start,
                end = end,
                stream_name = 'test-1',
            )
        case _:
            raise exceptions.EventerException('action not implemented')

    main(**args)
