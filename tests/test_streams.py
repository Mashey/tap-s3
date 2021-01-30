import pytest

from tap_s3.streams import Stream

@pytest.fixture
def table_spec(people_table_config):
    return people_table_config['people']


@pytest.fixture
def people_csv_data():
    return """id,name,age
    0,dan,10
    1,ana,
    """


@pytest.fixture
def records():
    return [
        {'id': 0, 'name': 'dan', 'age': 10.0},
        {'id': 1, 'name': 'ana', 'age': None}
    ]


@pytest.fixture
def bucket(s3, bucket_1_name, people_csv_data, houses_1_csv_data):
    bucket = s3.create_bucket(Bucket=bucket_1_name)
    bucket.put_object(Body=people_csv_data, Key='prefix_1/20201125/people.csv')
    bucket.put_object(Body=houses_1_csv_data, Key='prefix_1/20201125/houses.csv')
    yield


@pytest.fixture
def s3_client(s3, bucket):
    yield s3


@pytest.fixture
def state():
    return None


@pytest.fixture
def stream(client, table_spec, state):
    yield Stream(client, table_spec, state)


def test_initialization(stream, client, table_spec, state):
    assert stream.client == client
    assert stream.state == state
    assert stream.bucket_name == table_spec['bucket_name']
    assert stream.search_prefix == table_spec['search_prefix']
    assert stream.search_pattern == table_spec['search_pattern']
    assert stream.delimiter == table_spec['delimiter']
    assert stream.tap_stream_id == table_spec['tap_stream_id']
    assert stream.key_properties == table_spec['primary_key']
    assert stream.replication_method == table_spec['replication_method']
    assert stream.valid_replication_keys == table_spec['valid_replication_keys']
    assert stream.replication_key == table_spec['replication_key']
    assert stream.object_type == table_spec['object_type']


def test_get_schema(s3_client, stream, people_schema):
    assert stream.get_schema() == people_schema


def test_sync(s3_client, stream, records):
    sync_records = list(stream.sync())
    assert sync_records == records
    