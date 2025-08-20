# Simple code for checking connection to AWS

import boto3
from botocore.exceptions import BotoCoreError, ClientError

try:
    sts = boto3.client('sts')
    response = sts.get_caller_identity()
    print("Connected to AWS successfully.")
except (BotoCoreError, ClientError) as e:
    print("Failed to connect to AWS.")
    print(e)








