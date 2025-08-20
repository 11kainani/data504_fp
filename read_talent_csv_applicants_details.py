import boto3
import pandas as pd
from io import StringIO

# -------------------------------
# Initialize S3 client
# -------------------------------
s3 = boto3.client('s3')
bucket_name = 'data-504-final-project'
prefix = 'Talent/'  # folder path where CSVs are stored
out_prefix = 'Talent_Combined/'  # output folder path

# -------------------------------
# List all CSV files in the bucket/folder
# -------------------------------
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

csv_files = []
for page in pages:
    for obj in page.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            csv_files.append(obj['Key'])

if not csv_files:
    print("No CSV files found in the folder.")
else:
    print(f"Found {len(csv_files)} CSV files.")

    # -------------------------------
    # Combine all CSVs into one DataFrame
    # -------------------------------
    combined_df = pd.DataFrame()

    for file_key in csv_files:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(obj['Body'])
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    print("All CSV files combined successfully.")
    print(combined_df.head())

    # -------------------------------
    # Save combined CSV directly to S3 (no local file)
    # -------------------------------
    csv_buffer = StringIO()
    combined_df.to_csv(csv_buffer, index=False)

    s3_key = out_prefix + "combined_applicants_details.csv"  # S3 destination file
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())

    print(f"Combined CSV uploaded to s3://{bucket_name}/{s3_key}")

