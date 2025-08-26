# Combine academy CSVs by course (Business, Data, Engineering)
# Adds new columns: course, cohort_number, start_date
# Uploads 3 combined files back to S3

import boto3, io, pandas as pd, os, re

bucket = "data-504-final-project-v2"
prefix = "Academy/"
s3 = boto3.client("s3")

# 1) Get all CSV files from Academy/ folder
keys = []
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".csv"):
            keys.append(obj["Key"])

# 2) Group files into Business, Data, Engineering
groups = {"Business": [], "Data": [], "Engineering": []}
for k in keys:
    fname = os.path.basename(k)
    if fname.startswith("Business_"):
        groups["Business"].append(k)
    elif fname.startswith("Data_"):
        groups["Data"].append(k)
    elif fname.startswith("Engineering_"):
        groups["Engineering"].append(k)

# 3) Regex to capture course, cohort, start_date from filenames
pattern = re.compile(r"([A-Za-z]+)_(\d+)_(\d{4}-\d{2}-\d{2})\.csv")

# Function: combine multiple CSVs into one DataFrame
def combine(keys_list):
    frames = []
    for k in keys_list:
        fname = os.path.basename(k)
        match = pattern.match(fname)
        if not match:
            print(f"Skipping (bad filename): {fname}")
            continue

        course, cohort, start = match.groups()
        body = s3.get_object(Bucket=bucket, Key=k)["Body"].read()
        df = pd.read_csv(io.BytesIO(body))

        # Add new columns
        df["course"] = course
        df["cohort_number"] = int(cohort)
        df["start_date"] = pd.to_datetime(start)

        # reorder so new cols come right after name + trainer
        cols = list(df.columns)
        fixed_order = ["name", "trainer", "course", "cohort_number", "start_date"]
        other_cols = [c for c in cols if c not in fixed_order]
        df = df[fixed_order + other_cols]

        frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# 4) Build combined DataFrames
df_business    = combine(groups["Business"])
df_data        = combine(groups["Data"])
df_engineering = combine(groups["Engineering"])

# 5) Quick check for each combined file
for name, df in [("Business", df_business), ("Data", df_data), ("Engineering", df_engineering)]:
    print(f"\n{name} DataFrame → {df.shape[0]} rows, {df.shape[1]} cols")
    print("Has columns:", all(col in df.columns for col in ["course", "cohort_number", "start_date"]))
    print(df[["course", "cohort_number", "start_date"]].head())

# 6) Upload helper
def upload_df(df, key):
    if df.empty:
        print(f"Skip upload: no rows for {key}")
        return
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), ContentType="text/csv")
    print(f"Uploaded → s3://{bucket}/{key}  ({len(df)} rows)")

# 7) Upload results back to S3
upload_df(df_business,"Academy_Combined/Business_combined.csv")
upload_df(df_data,"Academy_Combined/Data_combined.csv")
upload_df(df_engineering,"Academy_Combined/Engineering_combined.csv")
