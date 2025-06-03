import pandas as pd
import pytest
import re

# Define a pytest fixture to create sample data
@pytest.fixture
def sample_phone_data():
    return pd.DataFrame(
        {
            "phone_number": [
                "+91 -9876543210",
                "+91 -1234567890",
                "+91-9876543215",
                "9876543210",
                "+91- 9876543210",
            ]
        }
    )


# Define the test function for phone number validation
def test_phone_number_format(sample_phone_data):
    """
    This test ensures that phone numbers follow the correct format:
    '+91 -XXXXXXXXXX' where X is a digit.
    """
    pattern = r"^\+91\s-\d{10}$"  # Expected pattern: +91 -XXXXXXXXXX

    # Apply regex check for each phone number
    sample_phone_data["valid_format"] = sample_phone_data["phone_number"].apply(lambda x: bool(re.match(pattern, x)))

    # Assert that all phone numbers match the expected pattern
    assert sample_phone_data["valid_format"].all(), "❌ Some phone numbers do not match the expected format!"
