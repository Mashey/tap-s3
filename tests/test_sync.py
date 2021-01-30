from singer import catalog
from tap_s3.discover import *
from tap_s3.sync import *
import pytest

@pytest.fixture
def s3_client(s3, bucket_1):
    yield s3

def test_sync(s3_client, config):
    catalog = discover(config)

    sync(config, {}, catalog)
    assert True
