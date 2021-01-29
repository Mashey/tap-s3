import boto3
from dotenv import load_dotenv
from moto import mock_s3
from tap_s3.client import S3Client
import os
import pytest

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

@pytest.fixture()
def bucket_name():
    return 'test_bucket'

@pytest.fixture()
def s3_bucket_with_objects(s3_client, bucket_name):
    bucket = s3_client.create_bucket(Bucket=bucket_name)
    bucket.put_object(Body=b'hello', Key='prefix_1/hello.csv')
    bucket.put_object(Body=b'world', Key='prefix_1/world.csv')
    bucket.put_object(Body=b'hello', Key='prefix_2/hello.csv')
    yield s3_client

@pytest.fixture()
def client():
    yield S3Client(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

def test_it_creates_a_valid_session(client):
    assert type(client._session) == boto3.Session
    assert client._s3 is not None

def test_get_bucket(bucket_name, client):
    bucket = client.get_bucket(bucket_name)
    assert bucket.name == bucket_name

def test_get_objects(bucket_name, s3_bucket_with_objects, client):
    bucket = client.get_bucket(bucket_name)
    objects = client.get_objects(bucket)
    assert list(objects) == list(s3_bucket_with_objects.Bucket(bucket_name).objects.all())

    filtered_objects = client.get_objects(bucket, 'prefix_1')
    assert list(filtered_objects) == list(s3_bucket_with_objects.Bucket(bucket_name).objects.filter(Prefix='prefix_1'))

def test_get_schema(client):
    schema = client.get_schema(
        'restaurant-exports',
        'GregoryCoffeeUSer',
        'CashEntries',
        '/')
    assert True