#!/usr/bin/env python
# coding: utf-8

#import packages
import boto3
import pandas as pd
from io import StringIO

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

    # Show cleaned dataset
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df)

    return df, bucket_name

def upload_cleaned(df, bucket_name, original_file_key):
    s3 = boto3.client("s3")

    # Create new key
    filename = original_file_key.split('/')[-1]
    new_key = f"Cleaned/{filename}"

    # Save dataframe to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3.put_object(Bucket=bucket_name, Key=new_key, Body=csv_buffer.getvalue())
    print(f"Cleaned file uploaded to s3://{bucket_name}/{new_key}")

if __name__ == "__main__":
    file_key = input("Enter the S3 file key (path inside the bucket): ")
    df, bucket_name = clean_dataset(file_key)
    upload_cleaned(df, bucket_name, file_key)
