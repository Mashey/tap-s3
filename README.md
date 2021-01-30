Config


```json
{
    "aws_access_key_id": "key",
    "aws_secret_access_key": "secret",
    "tables": {
        "people": {
            "bucket_name": "bucket_name",
            "search_prefix": "prefix",
            "search_pattern": "people.csv",
            "delimiter": "/",
            "tap_stream_id": "people",
            "primary_key": ["id"],
            "replication_method": "FULL_TABLE",
            "valid_replication_keys": [""],
            "replication_key": "",
            "object_type": "PERSON"
        },
        "houses": {
            "bucket_name": "bucket_name",
            "search_prefix": "prefix_1",
            "search_pattern": "houses.csv",
            "delimiter": "/",
            "tap_stream_id": "houses",
            "primary_key": ["id"],
            "replication_method": "INCREMENTAL",
            "valid_replication_keys": [""],
            "replication_key": "",
            "object_type": "HOUSE"
        }
    }
}
```

