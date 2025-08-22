import boto3
import pandas as pd
import io

class S3DataFrames:
    def __init__(self):
        self.bucket_name = 'data-504-final-project'
        self.prefix = 'Academy_Combined'
        self.s3_client = boto3.client('s3')

    def get_csvs(self):
        dfs = {}
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.prefix + "/")
        for obj in response.get("Contents", []):
            key = obj['Key']
            if key.endswith(".csv"):
                csv_obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                df = pd.read_csv(io.StringIO(csv_obj['Body'].read().decode('utf-8')))
                df.columns = df.columns.str.strip()  # Normalize column names
                dfs[key.split("/")[-1].replace(".csv", "")] = df
        return dfs

s3dfs = S3DataFrames()

dfs = s3dfs.get_csvs()

print(dfs)