import pandas as pd
import pytest


@pytest.fixture
def sample_orders_data():
    return pd.DataFrame(
        {
            "OrderDate": [
                "2024-06-15 18:30:00+00:00",
                "1993-07-21 01:09:00+00:00",
                None,
            ],
            "ShippingDate": [
                "2024-06-26 14:05:00+00:00",
                "1993-07-29 07:06:00+00:00",
                "2024-06-27 08:00:00+00:00",
            ],
            "ExpectedDeliveryDate": [
                "2024-07-15 19:48:00+00:00",
                "1993-08-06 22:24:00+00:00",
                "2024-07-05 12:00:00+00:00",
            ],
            "ActualDeliveryDate": [
                "2024-08-10 11:00:00+00:00",
                "1993-08-27 12:01:00+00:00",
                None,
            ],
        }
    )


# ✅ Test 1: Ensure all timestamps are valid format
def test_timestamp_format(sample_orders_data):
    for col in [
        "OrderDate",
        "ShippingDate",
        "ExpectedDeliveryDate",
        "ActualDeliveryDate",
    ]:
        sample_orders_data[col] = pd.to_datetime(sample_orders_data[col], errors="coerce")
        assert not sample_orders_data[col].isnull().all(), f"❌ {col} contains all invalid timestamps"
