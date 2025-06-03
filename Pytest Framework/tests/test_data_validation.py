import pytest

@pytest.fixture
def sample_data():
    return {"name": "John", "age": 30, "city": "NYC"}

def test_data_validation(sample_data):
    assert sample_data["age"] > 0