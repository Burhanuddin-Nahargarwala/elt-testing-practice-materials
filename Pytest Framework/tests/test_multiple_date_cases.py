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


# ✅ Test 2: Ensure no future dates in OrderDate
def test_no_future_dates_in_order_date(sample_orders_data):
    sample_orders_data["OrderDate"] = pd.to_datetime(sample_orders_data["OrderDate"], errors="coerce")
    assert sample_orders_data["OrderDate"].max() <= pd.Timestamp.now(tz='UTC'), "❌ OrderDate contains future dates"

# ✅ Test 3: Ensure no past dates in ExpectedDeliveryDate
def test_no_past_dates_in_expected_delivery_date(sample_orders_data):
    sample_orders_data["ExpectedDeliveryDate"] = pd.to_datetime(sample_orders_data["ExpectedDeliveryDate"], errors="coerce")
    assert sample_orders_data["ExpectedDeliveryDate"].min() >= pd.Timestamp.now(tz='UTC'), "❌ ExpectedDeliveryDate contains past dates"

# ✅ Test 4: Ensure no null values in OrderDate
def test_no_null_values_in_order_date(sample_orders_data):
    sample_orders_data["OrderDate"] = pd.to_datetime(sample_orders_data["OrderDate"], errors="coerce")
    assert not sample_orders_data["OrderDate"].isnull().any(), "❌ OrderDate contains null values"