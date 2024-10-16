"""
Consumer interface
"""
import os
import logging

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

class Client:
    """docs"""
    def __init__(self, **params):
        self.logger = logging.getLogger(__name__)
        for key, val in params.items():
            setattr(self, key, val)
    
    # If we're mixing consumer and replayer, interface will
    # need to be pretty generic. Should break out consumer and replayer.
    def consume(self):
        """docs"""
        raise NotImplementedError
    
    def replay(self):
        """docs"""
        raise NotImplementedError