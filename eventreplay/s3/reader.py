"""
Handle reading of messages saved in S3.
"""
import os
import re
import logging
from datetime import datetime, timezone

import boto3

from eventreplay import exceptions


PATTERN = r"(\d{4}/\d{2}/\d{2}/\d{2}/\d{2})"

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'))

class Reader():
    """
    Reader
    """
    def __init__(self, bucket, start, end):
        self.client = boto3.client('s3')
        self.resource = boto3.resource('s3')
        self.logger = logging.getLogger(__name__)
        self.bucket = bucket
        self.start = self._string_to_datetime(start)
        self.end = self._string_to_datetime(end)
        self.prefix = self._common_prefix(start, end)
    
    def read(self):
        """
        Read messages
        """
        files = self._files()
        for key in files:
            response = self.client.get_object(Bucket=key.bucket_name, Key=key.key) 
            object_data = response['Body']
            yield object_data.read()

    @staticmethod
    def _string_to_datetime(dt):
        return datetime.strptime(dt, '%Y/%m/%d/%H/%M').replace(tzinfo=timezone.utc)

    @staticmethod    
    def _strings_to_datetime(*dts):
        return [Reader._string_to_datetime(dt) for dt in dts]
    
    def _common_prefix(self, start, end):
        prefix = ''
        for char in zip(start, end):
            if len(set(char)) == 1:
                prefix += char[0]
            else:
                break
        prefix = prefix.rpartition('/')[0] + '/'
        return prefix
        
    
    def _files(self):
        bucket = self.resource.Bucket(self.bucket)
        candidates = bucket.objects.filter(Prefix=self.prefix)
        for obj in candidates:
            match = re.search(PATTERN, obj.key)
            if match:
                ts = self._string_to_datetime(match.group(0))
                if self.start <= ts <= self.end:
                    yield obj
            else:
                raise exceptions.S3KeyException('regex failed - invalid s3 key')
            
        