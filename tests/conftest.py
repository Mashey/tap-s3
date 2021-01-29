import boto3
import os
import pytest

from moto import mock_s3, mock_sqs


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing_secret"


@pytest.fixture
def s3_client(aws_credentials):
    with mock_s3():
        session = boto3.Session(region_name='us-east-1')
        yield session.resource('s3')
