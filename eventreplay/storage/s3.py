"""
Handle reading of messages saved in S3.
"""
import os
import re
import json
import logging
from datetime import datetime, timezone
from dataclasses import dataclass

import boto3

from eventreplay import exceptions


PATTERN = r"(\d{4}/\d{2}/\d{2}/\d{2}/\d{2})"

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'))

S3_CLIENT = boto3.client('s3', region_name='us-west-2')

@dataclass
class File():
    name: str
    timestamp: str
    content: dict

@dataclass
class Packet():
    bucket: str
    prefix: str
    files: list


class Writer():
    def __init__(self, eventer, bucket):
        self.eventer = eventer
        self.bucket = bucket
        # self.packets = typing.Dict[str, File] # TODO: get typing working here
        self.packets = {}
        self.client = S3_CLIENT
        self.logger = logging.getLogger(__name__)
    
    def write(self):
        """docstring"""
        for ts, files in self.packets.items():
            prefix = f'{self.eventer}/{ts}'
            for file in files:
                try:
                    body = json.dumps(file.content, default=lambda o: o.__dict__, indent=2).encode('utf-8')
                    print('body:', body)
                    key = f'{prefix}/{file.name}'
                    self.client.put_object(Body=body, Bucket=self.bucket, Key=key)
                except Exception as e:
                    self.logger.error('Error saving to s3: %s ', e)
            self.logger.info('writing files to: s3://%s/%s/ ', self.bucket, prefix)

    def buffer(self, file: File):
        """docstring"""
        if file.timestamp not in self.packets:
            self.packets[file.timestamp] = [file]
        else:
            self.packets[file.timestamp].append(file)
    
    @classmethod
    def from_sqs(cls, bucket):
        return cls('sqs', bucket)


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
            
        