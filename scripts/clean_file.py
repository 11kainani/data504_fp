#!/usr/bin/env python
# coding: utf-8

import boto3
import pandas as pd

def clean_dataset(file_key):
    s3 = boto3.client("s3")
    bucket_name = "data-504-final-project-v2"

    # Read CSV from S3
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_csv(obj["Body"])


    # Convert 'Date' column to datetime
    df['Date'] = pd.to_datetime(df['Date'], format="%A %d %B %Y", errors='coerce')

    # Convert 'Name' column to title case
    df['Name'] = df['Name'].str.title()

    # Show cleaned dataset in full
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df)

    return df

if __name__ == "__main__":
    file_key = input("Enter the S3 file key (path inside the bucket): ")
    clean_dataset(file_key)