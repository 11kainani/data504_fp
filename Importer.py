import json

import boto3
import pandas as pd

from Talent import Talent


class Importer:
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.bucket = 'data-504-final-project'
        self.talent_keys = []

    def bucket_key_names(self):
        """
        Get the names of the bucket keys.
        :return: bucket keys
        """
        buckets = self.s3.list_objects(Bucket=self.bucket)['Contents']
        keys = [content['Key'] for content in buckets]
        return keys


    def import_s3_file(self,key):
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        obj = json.loads(obj['Body'].read())
        print(obj)
        talent = Talent(obj)
        print(talent.get_info())

    def key_classification(self,keys):
        for key in keys:
            if key.startswith('Talent') and key.endswith('.json'):
                self.talent_keys.append(key)
if __name__ == '__main__':
    imp = Importer()
    keys = imp.bucket_key_names()
    imp.key_classification(keys)
    print(keys)
