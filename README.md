# Outline
Library for event listening/consuming and optionally replaying messages. Messages are stored in s3 with temporal partitioning. You can enable/disable this storage option. If enabled, you can replay messages for a given time range.

This is a work in progress.

Consume from a queue like so:
```python
client = sqs.client(queue_name=queue_name,
                    account_id=account_id,
                    persist_messages=True,
                    message_store='s3',
                    storage_destination='my-bucket')

for message in client.consume():
    # Do message processing here. This will run indefinitely
    print('msg received: ', message)
```

If these fields are set: `persist_messages`, `message_store`, `storage_destination` then messages will be saved given the storage specs. From the example above, messages will be consumed as usual, but they will also be stored with temporal partition based on the time in which the event originated. Events are grouped by minute. `s3://my-bucket/2024/10/15/17/08/<message_id>`. 

For now, can only replay to minute range - eg can only replay events from say `start=202410150800` to `end=202410150801`. This will replay messages from 08:00 to 08:01. You can't more granular than this, for now.

You can then replay with
```python
client = sqs.client(action='replay')
client.replay(start = 202410150800, end = 202410150801)
```

# testing
`./scripts/worker-init.sh sqs` requires SQS queue, s3 bucket
`./scripts/replayer-init.sh sqs` requires SQS queue, s3 bucket
`./scripts/worker-init.sh kinesis` requires kinesis stream, s3 bucket
`./scripts/replayer-init.sh kinesis` requires kinesis stream, s3 bucket