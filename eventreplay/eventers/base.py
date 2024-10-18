"""
Consumer interface
"""
import os
import logging

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

class ConsumerClient:
    """docs"""
    def __init__(self, **params):
        self.logger = logging.getLogger(__name__)
        for key, val in params.items():
            setattr(self, key, val)
    
    def consume(self):
        """docs"""
        raise NotImplementedError
    

class ReplayerClient:
    """docs"""
    def __init__(self, **params):
        self.logger = logging.getLogger(__name__)
        for key, val in params.items():
            setattr(self, key, val)

    def replay(self, start, end, bucket):
        """docs"""
        raise NotImplementedError