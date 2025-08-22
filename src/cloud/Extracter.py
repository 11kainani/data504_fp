import json

import boto3
import pandas as pd
import re


class Extracter:
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.bucket = 'data-504-final-project-v2'
        self.talent_keys = []
        self.academy_keys = []
        self.talent_applications = []
        self.sparta_days = []
        self.courses_list = {'Business', 'Data','Engineering'}
        self.skills_list = {'Analytic', 'Independent','Determined','Professional','Studious','Imaginative'}
        self.talent_combined = []
        key_list = self.bucket_key_names()
        self.key_classification(key_list)


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
            if key.startswith('Academy/') and key.endswith('.csv'):
                self.academy_keys.append(key)
            if key.startswith('Talent/') and key.endswith('Applicants.csv'):
                self.talent_applications.append(key)
            if key.startswith('Talent/Sparta') and key.endswith('.txt'):
                self.sparta_days.append(key)
            if key.startswith('Talent_Combined') and key.endswith('.csv'):
                self.talent_combined.append(key)


    @staticmethod
    def academy_table_transformer(key_list):
        filename = key_list.split('/')[-1]
        match = re.match(r'(.+)_(\d+)_(\d{4}-\d{2}-\d{2})\.csv', filename)
        course_name, course_number, start_date = match.groups()
        print(course_name, course_number, start_date)
        return {'course_name':course_name, 'course_number':course_number, 'start_date':start_date}


if __name__ == '__main__':
    imp = Extracter()

    keys = imp.bucket_key_names()
    #print(keys)

    imp.key_classification(keys)
    print(imp.talent_combined)
    data = imp.import_s3_csv_file('Talent_Combined/Cleaned/combined_sparta_day_test_score.csv')
    print(data.info())
    data.to_csv('cleaned_sparta_day.csv')
    #print(imp.talent_applications)

    #data = imp.import_s3_csv_file('Talent/Jan2019Applicants.csv')
    #print(data.columns)
    #data.to_csv('../files/Jan2019Applicants.csv')