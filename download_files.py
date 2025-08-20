import boto3

s3 = boto3.client("s3")

bucket_name = "data-504-final-project"
file_key = "Academy_Combined/Engineering_combined.csv"
local_file = "csv_files/Engineering_combined.csv"

s3.download_file(bucket_name, file_key, local_file)
print(f"File downloaded: {local_file}")