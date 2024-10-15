"""
Example implementation - Replay messages to designated eventing service
"""
import os
import logging

from eventreplay import consumers

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

S3_BUCKET = 'event-replay-3jxh'
QUEUE_NAME = 'eventreplay'

# only handles utc for now
def handler(start, end, timezone='utc', eventer='sqs', action='replay', **kwargs):
    """
    SQS eventer
    """
    logger.info('Starting eventer %s\n', eventer)
    # TODO: make handler eventer agnostic - build `args` more with kwargs
    # eg - helper func can specify kwargs.get('queue') when eventer is sqs
    # this is sqs specific implementation
    args = {**kwargs, **dict(action = action)}
    eventers = {x: consumers.client(x, **args) for x in ['sqs']}
    client = eventers[eventer]
    client.replay(start, end, S3_BUCKET)
    

if __name__ == "__main__":
    # add dry-run flag
    start = "2024/10/15/17/00" # yyyy/mm/dd/hh/mm
    end = "2024/10/15/19/30"
    args = dict(
        queue = "fake-service",
    )
    handler(start, end, **args)
