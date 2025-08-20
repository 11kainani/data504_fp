import boto3
import pandas as pd

s3 = boto3.client("s3")

bucket_name = "data-504-final-project"
file_key = "Talent_Combined/Cleaned/combined_sparta_day_test_score.csv"

obj = s3.get_object(Bucket=bucket_name, Key=file_key)

# Load entire CSV into memory
df = pd.read_csv(obj["Body"])
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(df)
