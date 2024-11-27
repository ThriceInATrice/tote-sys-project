from src.process_data.get_fact_sales_order import get_fact_sales_order
import pytest


def test_list_processed_correctly():

    test_list = [
        {
            "sales_order_id": 1,
            "created_at": "2024-09-01 12-12-12.000000",
            "last_updated": "2024-09-01 12-12-12.000000",
            "design_id": 1,
            "staff_id": 2,
            "counterparty_id": 3,
            "units_sold": 2,
            "unit_price": 3,
            "currency_id": 3,
            "agreed_delivery_date": "2024-10-11 17-00-00.000000",
            "agreed_payment_date": "2024-10-10 17-00-00.000000",
            "agreed_delivery_location_id": "4",
        },
        {
            "sales_order_id": 1,
            "created_at": "2024-08-01 12-12-12.000000",
            "last_updated": "2024-08-01 12-12-12.000000",
            "design_id": 1,
            "staff_id": 2,
            "counterparty_id": 3,
            "units_sold": 2,
            "unit_price": 55,
            "currency_id": 889,
            "agreed_delivery_date": "2025-02-01 12-00-00-000011",
            "agreed_payment_date": "2025-01-15 13-05-30-123456",
            "agreed_delivery_location_id": "4",
        },
    ]

    expected_output = [
        {
            "created_date": "20240901",
            "created_time": "12-12-12.000000",
            "last_updated_date": "20240901",
            "last_updated_time": "12-12-12.000000",
            "design_id": 1,
            "staff_id": 2,
            "counterparty_id": 3,
            "units_sold": 2,
            "unit_price": 3,
            "currency_id": 3,
            "agreed_delivery_date": "20241011",
            "agreed_payment_date": "20241010",
            "agreed_delivery_location_id": "4",
        },
        {
            "created_date": "20240801",
            "created_time": "12-12-12.000000",
            "last_updated_date": "20240801",
            "last_updated_time": "12-12-12.000000",
            "design_id": 1,
            "staff_id": 2,
            "counterparty_id": 3,
            "units_sold": 2,
            "unit_price": 55,
            "currency_id": 889,
            "agreed_delivery_date": "20250201",
            "agreed_payment_date": "20250115",
            "agreed_delivery_location_id": "4",
        },
    ]

    assert get_fact_sales_order(test_list) == expected_output
