#!/usr/bin/env python
# coding: utf-8

import boto3
import pandas as pd

def main(file_key):
    s3 = boto3.client("s3")
    bucket_name = "data-504-final-project-v2"

    # Read file from S3
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_csv(obj["Body"])

    # Print entire dataframe
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df)

if __name__ == "__main__":
    # Ask the user for the file key
    file_key = input("Enter the S3 file key (path inside the bucket): ")
    main(file_key)