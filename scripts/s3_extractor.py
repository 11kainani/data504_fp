import boto3
import pandas as pd
import io

class S3Extractor:
    """
    Class for extracting data from the S3 bucket 'data-504-final-project-v2'.
    """

    def __init__(self):
        """
        Initializes S3 client, bucket name, and target folders.
        """
        self.bucket_name = 'data-504-final-project-v2'
        self.folders = ['Academy_Combined/', 'Talent_Combined/']
        self.s3_client = boto3.client('s3')

    def get_csvs_to_dfs(self):
        """
        Extracts CSV files from the specified folders and converts them into DataFrames.

        Returns:
            dict: Dictionary of DataFrames keyed by filename without extension.
        """
        dfs = {}
        for folder in self.folders:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=folder)
            for obj in response.get("Contents", []):
                key = obj['Key']
                if key.endswith(".csv"):
                    csv_obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                    df = pd.read_csv(io.StringIO(csv_obj['Body'].read().decode('utf-8')))
                    dfs[key.split("/")[-1].replace(".csv", "")] = df
        return dfs