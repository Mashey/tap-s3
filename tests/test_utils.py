import io
import pandas as pd
import numpy as np
import pytest

from tap_s3.utils import *

@pytest.fixture
def people_csv_data():
    return """id,name,age,cash
    0,dan,10,11.0
    1,ana,,50.4
    2,,13,1.1
    """

@pytest.fixture
def records():
    return [
        {'id': 0, 'name': 'dan', 'age': 10, 'cash': 11.0},
        {'id': 1, 'name': 'ana', 'age': None, 'cash': 50.4},
        {'id': 2, 'name': None, 'age': 13, 'cash': 1.1}
    ]

@pytest.fixture
def raw_people_df(people_csv_data):
    yield pd.read_csv(
        io.StringIO(people_csv_data),
        index_col=None,
        dtype=str)

def test_infer():
    assert infer_type('10.0') == float
    assert infer_type('10') == int
    assert infer_type('1000004213') == int
    assert infer_type('Hello') == str
    assert infer_type('1Hello') == str
    assert infer_type('He11o') == str
    assert infer_type('') == str

def test_clean_dataframe(raw_people_df, records):
    cleaned_df = clean_dataframe(raw_people_df)
    cleaned_records = cleaned_df.replace({np.nan:None}).to_dict('records')

    assert cleaned_records == records


