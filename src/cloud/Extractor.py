import json

import boto3
import pandas as pd
import re


class Extractor:
    """
    This class is responsible for extracting data from the cloud. Most the interaction from the S3 bucket will be made here
    """
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.bucket = 'data-504-final-project-v2' # Could be used as a param for the init but has been hard coded because this implementation as been designed for this project
        self.talent_keys = []
        self.academy_keys = []
        self.talent_applications = []
        self.sparta_days = []
        self.courses_list = {'Business', 'Data','Engineering'} # Courses names but not really need since the creation the course is done automatically using the key name from the bucket
        self.skills_list = {'Analytic', 'Independent','Determined','Professional','Studious','Imaginative'} #Names of the skills but also not as important since the skill creation has been automatised

        self.talent_combined = []

        key_list = self.bucket_key_names() # Get the names of all the keys (files) in the bucket
        self.key_classification(key_list) # classifies all the keys into the correct list


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
        """
        Reads json file from the cloud inside the bucket
        :param key:
        :return: dataframe with the content of the file
        """
        file = self.s3.get_object(Bucket=self.bucket, Key=key)
        obj = json.loads(file['Body'].read())
        return obj

    def import_s3_csv_file( self, key) -> pd.DataFrame:
        """
        Reads csv file from the cloud inside the bucket
        :param key:
        :return: Dataframe with the content of the file
        """
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
            if key.startswith('Academy/') and key.endswith('.csv'):
                self.academy_keys.append(key)
            if key.startswith('Talent/') and key.endswith('Applicants.csv'):
                self.talent_applications.append(key)
            if key.startswith('Talent/Sparta') and key.endswith('.txt'):
                self.sparta_days.append(key)
            if key.startswith('Talent_Combined') and key.endswith('.csv'):
                self.talent_combined.append(key)

if __name__ == '__main__':

    imp = Extractor()

    keys = imp.bucket_key_names()
    print(keys)

    imp.key_classification(keys)
    print(imp.talent_combined)
    data = imp.import_s3_csv_file('Talent_Combined/Cleaned/combined_sparta_day_test_score.csv')
    print(data.info())
    data.to_csv('cleaned_sparta_day.csv')
    #print(imp.talent_applications)

    #data = imp.import_s3_csv_file('Talent/Jan2019Applicants.csv')
    #print(data.columns)
    #data.to_csv('../files/Jan2019Applicants.csv')