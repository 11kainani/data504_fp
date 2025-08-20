import json

import boto3
import pandas as pd

from Talent import Talent


class Extracter:
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.bucket = 'data-504-final-project'
        self.talent_keys = []
        self.business_keys = []
        self.data_keys = []
        self.engineering_keys = []
        self.talent_applications = []
        self.sparta_days = []



    def bucket_key_names(self):
        """
        Get the names of the bucket keys.
        :return: bucket keys
        """
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    keys.append(obj["Key"])
        return keys


    def import_s3_json_file(self,key):
        file = self.s3.get_object(Bucket=self.bucket, Key=key)
        obj = json.loads(file['Body'].read())
        return obj

    def import_s3_csv_file( self, key) -> pd.DataFrame:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return pd.read_csv(obj['Body'])

    def key_classification(self, key_list):
        """
        Classifies the keys into list depending on the key type.
        :param key_list: list of the names of the keys in the bucket
        :return: None
        """
        for key in key_list:
            if key.startswith('Talent/') and key.endswith('.json'):
                self.talent_keys.append(key)
            if key.startswith('Academy/Business') and key.endswith('.csv'):
                self.business_keys.append(key)
            if key.startswith('Academy/Engineering') and key.endswith('.csv'):
                self.engineering_keys.append(key)
            if key.startswith('Academy/Data') and key.endswith('.csv'):
                self.data_keys.append(key)
            if key.startswith('Talent/') and key.endswith('Applicants.csv'):
                self.talent_applications.append(key)
            if key.startswith('Talent/Sparta') and key.endswith('.txt'):
                self.sparta_days.append(key)

if __name__ == '__main__':
    imp = Extracter()
    keys = imp.bucket_key_names()
    imp.key_classification(keys)
    print(keys)
    #data = imp.import_s3_csv_file('Academy/Data_36_2019-10-28.csv')
    #print(data.info())
    #print(imp.engineering_keys, imp.data_keys, imp.talent_keys, imp.talent_applications)