from src.process_data.get_fact_sales_order import get_fact_sales_order
import pytest


def test_list_processed_correctly():

    test_list = [
        {
            "sales_order_id": 1,
            "created_at": "2024-09-01 12-12-12.000000",
            "last_updated": "2024-09-01 12-12-12.000000",
            "design_id": 2,
            "staff_id": 3,
            "counterparty_id": 4,
            "units_sold": 5,
            "unit_price": 50.00,
            "currency_id": 6,
            "agreed_delivery_date": "2024-10-11 17-00-00.000000",
            "agreed_payment_date": "2024-10-10 17-00-00.000000",
            "agreed_delivery_location_id": 7,
        },
        {
            "sales_order_id": "8",
            "created_at": "2024-08-01 12-12-12.000000",
            "last_updated": "2024-08-01 12-12-12.000000",
            "design_id": "9",
            "staff_id": "10",
            "counterparty_id": "11",
            "units_sold": "12",
            "unit_price": "99.99",
            "currency_id": "13",
            "agreed_delivery_date": "2025-02-01 12-00-00-000011",
            "agreed_payment_date": "2025-01-15 13-05-30-123456",
            "agreed_delivery_location_id": "14",
        },
    ]

    expected_output = [
        {
            "sales_order_id": 1,
            'created_date': '2024-09-01',
            'created_time': '12-12-12',
            'last_updated_date': '2024-09-01',
            'last_updated_time': '12-12-12',
            'design_id': 2, 
            'sales_staff_id': 3,
            'counterparty_id': 4, 
            'units_sold': 5,
            'unit_price': 50.00, 
            'currency_id': 6,
            'agreed_delivery_date': '2024-10-11',
            'agreed_payment_date': '2024-10-10',
            'agreed_delivery_location_id': 7
        }, 
        {
            "sales_order_id": 8,
            'created_date': '2024-08-01',
            'created_time': '12-12-12',
            'last_updated_date': '2024-08-01',
            'last_updated_time': '12-12-12',
            'design_id': 9, 
            'sales_staff_id': 10,
            'counterparty_id': 11, 
            'units_sold': 12,
            'unit_price': 99.99, 
            'currency_id': 13,
            'agreed_delivery_date': '2025-02-01',
            'agreed_payment_date': '2025-01-15',
            'agreed_delivery_location_id': 14
        }
    ]

    assert get_fact_sales_order(test_list) == expected_output
