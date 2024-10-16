"""
Example implementation - Replay messages to designated eventing service
"""
import os
import logging

from eventreplay import eventers

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

S3_BUCKET = 'event-replay-3jxh'
QUEUE_NAME = 'eventreplay'

def handler(start, end, eventer='sqs', action='replay', **kwargs):
    """
    SQS eventer
    """
    logger.info('Starting eventer %s\n', eventer)
    # TODO: make handler eventer agnostic - build `args` more with kwargs
    # eg - helper func can specify kwargs.get('queue') when eventer is sqs
    # this is sqs specific implementation
    kwargs = {**kwargs, **dict(action = action)}
    clients = {x: eventers.client(x, **kwargs) for x in ['sqs']}
    client = clients[eventer]
    client.replay(start, end, S3_BUCKET)
    

if __name__ == "__main__":
    start = "2024/10/15/17/00" # yyyy/mm/dd/hh/mm - must be UTC for now
    end = "2024/10/15/19/30"
    args = dict(
        queue = "fake-service",
    )
    handler(start, end, **args)
