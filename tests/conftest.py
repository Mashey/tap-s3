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
def s3(aws_credentials):
    with mock_s3():
        session = boto3.Session(region_name='us-east-1')
        yield session.resource('s3')


@pytest.fixture
def people_1_csv_data():
    return """id,name,age
    0,bob,50
    1,jane,32
    """


@pytest.fixture
def people_2_csv_data():
    return """id,name,age
    3,bill,33
    4,jill,35
    """


@pytest.fixture
def people_schema():
    return {
        "type": ["null", "object"],
        "properties": {
            "id": {
                "type": ["null", "integer"]
            },
            "name": {
                "type": ["null", "string"]
            },
            "age": {
                "type": ["null", "integer"]
            }
        }
    }


@pytest.fixture
def houses_1_csv_data():
    return """id,person_id,state
    0,0,FL
    1,3,CO
    """


@pytest.fixture
def houses_2_csv_data():
    return """id,person_id,state
    3,0,CO
    4,1,IN
    """


@pytest.fixture
def houses_schema():
    return {
        "type": ["null", "object"],
        "properties": {
            "id": {
                "type": ["null", "integer"]
            },
            "person_id": {
                "type": ["null", "integer"]
            },
            "state": {
                "type": ["null", "string"]
            }
        }
    }


@pytest.fixture
def bucket_1_name():
    return 'test_bucket_1'


@pytest.fixture
def bucket_1(s3, bucket_1_name, people_1_csv_data, people_2_csv_data, houses_1_csv_data, houses_2_csv_data):
    bucket = s3.create_bucket(Bucket=bucket_1_name)
    bucket.put_object(Body=people_1_csv_data, Key='prefix_1/20201123/people.csv')
    bucket.put_object(Body=people_2_csv_data, Key='prefix_1/20201124/people.csv')
    bucket.put_object(Body=houses_1_csv_data, Key='prefix_1/20201123/houses.csv')
    bucket.put_object(Body=houses_2_csv_data, Key='prefix_2/20201123/houses.csv')
    yield


@pytest.fixture
def s3_client(s3, bucket_1):
    yield s3
