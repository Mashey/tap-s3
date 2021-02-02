import boto3
import numpy as np
import pandas as pd
import random

from .schema_builder import create_json_schema

class S3Client:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self._session = boto3.Session(
            aws_access_key_id=aws_access_key_id, 
            aws_secret_access_key=aws_secret_access_key)
        self._s3 = self._session.resource('s3')

    def get_bucket(self, bucket_name):
        return self._s3.Bucket(bucket_name)


    def get_objects(self, bucket, prefix=''):
        return bucket.objects.filter(Prefix=prefix)


    def filter_objects_by_pattern(self, objects, pattern):
        objs = []
        for object in objects:
            if pattern in object.key:
                objs.append(object)
        return objs

    def get_updated_objects(self, objects, last_modified):
        objs = []
        for object in objects:
            if object.last_modified >= last_modified:
                objs.append(object)
        return objs


    def get_schema(self, bucket_name, search_prefix, search_pattern, delimiter):
        bucket  = self.get_bucket(bucket_name)
        objects = self.filter_objects_by_pattern(
            self.get_objects(bucket, search_prefix),
            search_pattern)
        
        # read objects as dataframe
        dfs = []
        for obj in objects:
            dfs.append(pd.read_csv(obj.get()['Body'], index_col=None))
        df = pd.concat(dfs, ignore_index=True)
        
        # build the most complete json object as possible
        # row data doesn't matter, just types
        json_obj = {}
        for column, column_vals in df.iteritems():
            json_obj[column] = None
            for val in column_vals:
                if pd.notna(val):
                    json_obj[column] = val
                    break
        
        return create_json_schema(json_obj)
        