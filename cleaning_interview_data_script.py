import boto3
import pandas as pd
from io import StringIO

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


# Get data
response = s3_client.get_object(Bucket='data-504-final-project', Key='Talent_Combined/combined_talent_decision_scores.csv')
content = response['Body'].read().decode('utf-8')


# Create a df
df = pd.read_csv(StringIO(content))

#---------------------------------------------------------------


##### Missing values
print(df.isnull().sum())


##### Duplicates
# Detect duplicate rows
duplicates = df.duplicated()
print(f"Number of duplicates: {duplicates.sum()}")


# Remove duplicates
df = df.drop_duplicates()


##### Data types

### Dates
# Try converting to datetime (invalid → NaT)
temp = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce')

# Any rows that became NaT are invalid
invalid_rows = df[temp.isna()]
print(invalid_rows[['name', 'date']])

# Fix
df['date'] = df['date'].str.replace('//', '/', regex=False)

# Change to datetime
df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce')
temp = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce')

# Check if fixed
invalid_rows = df[temp.isna()]
print(invalid_rows[['name', 'date']])



### Booleans
bool_cols = ['self_development', 'geo_flex', 'financial_support_self']
for col in bool_cols:
    df[col] = df[col].map({'Yes': True, 'No': False})


### Object to Category
# result as categorical
df['result'] = df['result'].astype('category')

# course_interest as categorical
df['course_interest'] = df['course_interest'].astype('category')


### Strengths and weaknesses -> CSVs
# strengths and weaknesses to lists
df['strengths'] = df['strengths'].apply(lambda x: ', '.join(x))
df['weaknesses'] = df['weaknesses'].apply(lambda x: ', '.join(x))


### Rename columns
# Rename tech scores columns
df.columns = df.columns.str.replace('#', 'Sharp').str.replace('.', '_')


### Check
print(df.dtypes)

#---------------------------------------------------------------

##### Upload back to S3
# Convert DataFrame to CSV in memory
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

# Upload directly to S3
s3 = boto3.client("s3")
bucket_name = "data-504-final-project"
s3_key = "Talent_Combined/Cleaned/cleaned_talent_decision_scores.csv"

s3.put_object(
    Bucket=bucket_name,
    Key=s3_key,
    Body=csv_buffer.getvalue()
)

print(f"Uploaded DataFrame directly to s3://{bucket_name}/{s3_key}")







