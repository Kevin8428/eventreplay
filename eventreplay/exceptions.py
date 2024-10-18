class S3KeyException(Exception):
    """
    Invalid key
    """
    pass

class EventerException(Exception):
    """
    Generic exception for eventers
    """
    pass

class EventerConsumerException(Exception):
    """
    Consumer exception for eventers
    """
    pass

class EventerReplayerException(Exception):
    """
    Replayer exception for eventers
    """
    pass
