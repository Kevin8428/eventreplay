guid=$(cat /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z0-9' | fold -w 50 | head -n 1)
aws kinesis put-record \
    --stream-name test-1 \
    --data some-record-$(date +%s) \
    --partition-key $guid # random uuid with ensure even shard distribution
