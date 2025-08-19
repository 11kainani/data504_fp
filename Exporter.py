import boto3
import pandas as pd


#def upload_to_s3(s3 : boto3.client('s3'), df: pd.DataFrame, bucket, key) -> None:
#    csv_bytes = df.to_csv(index=False).encode('utf-8')
#    s3.put_object(Bucket=bucket, Key=key, Body=csv_bytes)
#    print("uploaded")
