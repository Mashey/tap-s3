import boto3
from moto import mock_s3
from tap_s3.client import S3Client
import pytest


@pytest.fixture()
def client():
    yield S3Client('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY')


def test_it_creates_a_valid_session(client):
    assert type(client._session) == boto3.Session
    assert client._s3 is not None


def test_get_bucket(bucket_1, bucket_1_name, client, s3_client):
    bucket = client.get_bucket(bucket_1_name)
    assert bucket == s3_client.Bucket(bucket_1_name)


def test_get_objects(bucket_1_name, s3_client, client):
    bucket = client.get_bucket(bucket_1_name)
    objects = client.get_objects(bucket)
    assert list(objects) == list(s3_client.Bucket(bucket_1_name).objects.all())

    filtered_objects = client.get_objects(bucket, 'prefix_1')
    assert list(filtered_objects) == list(s3_client.Bucket(bucket_1_name).objects.filter(Prefix='prefix_1'))


def test_get_schema(s3_client, bucket_1_name, people_schema, client):
    schema = client.get_schema(
        bucket_1_name,
        'prefix_1',
        'people',
        '/')
    assert schema == people_schema
