import csv
import numpy as np
import pandas as pd
import singer

class Stream:
    def __init__(self, client, table_spec, state=None):
        self.client                 = client
        self.state                  = state
        self.bucket_name            = table_spec['bucket_name']
        self.search_prefix          = table_spec['search_prefix']
        self.search_pattern         = table_spec['search_pattern']
        self.delimiter              = table_spec['delimiter']
        self.tap_stream_id          = table_spec['tap_stream_id']
        self.key_properties         = table_spec['primary_key']
        self.replication_method     = table_spec['replication_method']
        self.valid_replication_keys = table_spec['valid_replication_keys']
        self.replication_key        = table_spec['replication_key']
        self.object_type            = table_spec['object_type']

    def sync(self, last_modified, *args, **kwargs):
        bucket = self.client.get_bucket(self.bucket_name)
        objects = self.client.filter_objects_by_pattern(
            self.client.get_objects(bucket, self.search_prefix),
            self.search_pattern)
        objects = self.client.get_updated_objects(objects, last_modified)

        for object in objects:
            df = pd.read_csv(object.get()['Body'], index_col=None)
            records = df.replace({np.nan:None}).to_dict('records')
            for record in records:
                yield record

        singer.write_bookmark(
            self.state,
            self.tap_stream_id,
            'last_modified',
            singer.utils.strftime(singer.utils.now(), format_str=singer.utils.DATETIME_PARSE))
        singer.write_state(self.state)
            

    def get_schema(self):
        return self.client.get_schema(
            self.bucket_name,
            self.search_prefix,
            self.search_pattern,
            self.delimiter
        )
