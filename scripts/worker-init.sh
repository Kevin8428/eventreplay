#!/bin/bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# python3 worker.py
python3 worker.py --account_id $ACCOUNT_ID