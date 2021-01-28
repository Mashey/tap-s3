from dotenv import load_dotenv
from singer import catalog
from tap_s3.discover import *
from tap_s3.sync import *
import os
import pytest

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

@pytest.fixture()
def config():
    yield {
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "tables": {
            "cash_entries": {
                "bucket_name": "restaurant-exports",
                "search_prefix": "GregoryCoffeeUSer",
                "search_pattern": "CashEntries.csv",
                "delimiter": "/",
                "tap_stream_id": "cash_entries",
                "primary_key": ["Entry Id"],
                "replication_method": "INCREMENTAL",
                "valid_replication_keys": ["created_at"],
                "replication_key": "created_at",
                "object_type": "CASH_ENTRY"
            },
            "order_details": {
                "bucket_name": "restaurant-exports",
                "search_prefix": "GregoryCoffeeUSer",
                "search_pattern": "OrderDetails.csv",
                "delimiter": "/",
                "tap_stream_id": "order_details",
                "primary_key": ["Order Id"],
                "replication_method": "INCREMENTAL",
                "valid_replication_keys": ["created_at"],
                "replication_key": "created_at",
                "object_type": "ORDER_DETAIL"
            }
        }
    }

def test_sync(config):
    catalog = discover(config)

    sync(config, {}, catalog)
    assert True
