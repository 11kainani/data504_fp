import boto3
import pandas as pd
import json
from io import StringIO

pd.set_option('display.max_columns', None)

# -------------------------------
# Initialize S3 client
# -------------------------------
s3 = boto3.client('s3')
bucket_name = 'data-504-final-project'
prefix = 'Talent/'  # input folder path
out_prefix = 'Talent_Combined/'  # output folder path

# -------------------------------
# List all files in the folder
# -------------------------------
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

dfs = []

for obj in response.get('Contents', []):
    key = obj['Key']
    if key.endswith('.json'):  # only JSON files
        file_obj = s3.get_object(Bucket=bucket_name, Key=key)
        data = file_obj['Body'].read().decode('utf-8')
        json_data = json.loads(data)

        # Flatten JSON into DataFrame
        df = pd.json_normalize(json_data)
        dfs.append(df)

# -------------------------------
# Combine all DataFrames
# -------------------------------
final_df = pd.concat(dfs, ignore_index=True)

# -------------------------------
# Upload combined CSV directly to S3 (no local file)
# -------------------------------
csv_buffer = StringIO()
final_df.to_csv(csv_buffer, index=False)

s3_key = out_prefix + "combined_talent_decision_scores.csv"   # file path in S3
s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())

print(f"Combined CSV uploaded directly to s3://{bucket_name}/{s3_key}")