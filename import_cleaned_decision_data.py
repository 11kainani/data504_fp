import boto3

s3 = boto3.client("s3")

bucket_name = "data-504-final-project"
local_file = "cleaned_talent_decision_scores.csv"
s3_key = "Talent_Combined/Cleaned/cleaned_talent_decision_scores.csv"

s3.upload_file(local_file, bucket_name, s3_key)

print(f"Uploaded {local_file} to s3://{bucket_name}/{s3_key}")