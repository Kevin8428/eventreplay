#!/bin/bash

aws kinesis create-stream --stream-name test-1 --shard-count 2 --region us-west-2