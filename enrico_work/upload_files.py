import boto3

s3 = boto3.client("s3")

bucket_name = "data-504-final-project-v2"
local_file = "csv_files/combined_sparta_day_test_score.csv"
s3_key = "Talent_Combined/Cleaned/combined_sparta_day_test_score.csv"

s3.upload_file(local_file, bucket_name, s3_key)

print(f"Uploaded {local_file} to s3://{bucket_name}/{s3_key}")
