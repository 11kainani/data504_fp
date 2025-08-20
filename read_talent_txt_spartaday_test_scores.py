import boto3
import pandas as pd
import re
from io import StringIO

pd.set_option('display.max_columns', None)

# -------------------------------
# Initialize S3 client
# -------------------------------
s3 = boto3.client('s3')
bucket_name = 'data-504-final-project'
prefix = 'Talent/'  # folder with txt files
out_prefix = 'Talent_Combined/'  # output folder path

# -------------------------------
# List all TXT files in the folder
# -------------------------------
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

txt_files = []
for page in pages:
    for obj in page.get('Contents', []):
        if obj['Key'].endswith('.txt'):
            txt_files.append(obj['Key'])

print(f"Found {len(txt_files)} TXT files.")

# -------------------------------
# Regex pattern for applicants
# -------------------------------
pattern = re.compile(
    r"^(.*?) -\s*Psychometrics:\s*(\d+)/\d+,\s*Presentation:\s*(\d+)/\d+",
    re.IGNORECASE
)

all_data = []

# -------------------------------
# Loop through each file
# -------------------------------
for file_key in txt_files:
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    lines = obj['Body'].read().decode('utf-8').splitlines()

    current_date = None
    current_academy = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Detect date
        if re.search(r"\d{4}", line) and any(day in line for day in
                                             ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]):
            current_date = line
            continue

        # Detect academy
        if "Academy" in line:
            current_academy = line
            continue

        # Detect applicant
        match = pattern.match(line)
        if match:
            name = match.group(1).strip()
            psychometrics = int(match.group(2))
            presentation = int(match.group(3))
            all_data.append([current_date, current_academy, name, psychometrics, presentation])

# -------------------------------
# Build final combined DataFrame
# -------------------------------
df = pd.DataFrame(all_data, columns=["Date", "Academy", "Name", "Psychometrics", "Presentation"])

print(df.head(15))
print(f"\nExtracted {len(df)} applicants from {len(txt_files)} files.")

# -------------------------------
# Save combined dataset directly to S3
# -------------------------------
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

s3_key = out_prefix + "combined_sparta_day_test_score.csv"  # save in output folder
s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())

print(f"Combined data uploaded to s3://{bucket_name}/{s3_key}")