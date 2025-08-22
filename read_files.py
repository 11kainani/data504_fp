import boto3
import pandas as pd

s3 = boto3.client("s3")

bucket_name = "data-504-final-project-v2"
file_key = "Talent_Combined/Cleaned/cleaned_talent_decision_scores.csv"

obj = s3.get_object(Bucket=bucket_name, Key=file_key)

df = pd.read_csv(obj["Body"])
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(df)
