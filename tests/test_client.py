import boto3
from boto3.resources.base import ServiceResource
from botocore.stub import Stubber
from dotenv import load_dotenv
from tap_s3.client import S3Client
import os
import pytest

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

@pytest.fixture()
def buckets():
    yield {
        "Owner": {
            "DisplayName": "test_buckets",
            "ID": "EXAMPLE123"
        },
        "Buckets": [{
            "CreationDate": "2016-05-25T16:55:48.000Z",
            "Name": "Bucket_1"
        }]
    }

@pytest.fixture()
def client():
    yield S3Client(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

def test_it_creates_a_valid_session(client):
    assert type(client._session) == boto3.Session
    assert client._s3 is not None
