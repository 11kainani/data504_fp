import boto3

s3 = boto3.client("s3")

bucket_name = "data-504-final-project-v2"
file_key = "Talent_Combined/Cleaned/cleaned_talent_decision_scores.csv"
local_file = "cleaned_talent_decision_scores.csv"

s3.download_file(bucket_name, file_key, local_file)
print(f"File downloaded: {local_file}")