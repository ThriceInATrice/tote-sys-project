from src.process_data.get_fact_sales_order import get_fact_sales_order
import pytest

test_list= [
{
    "sales_order_id": 1,
    "created_at": '2025-09-01 12-12-12.000000',
    "last_updated": '2024-09-01 12-12-12.000000',
    "design_id": 1, 
    "staff_id": 2,
    "counterparty_id": 3,
    "units_sold": 2,
    "unit_price": 3,
    "currency_id": 3,
    "agreed_delivery_date": 'datestr',
    "agreed_payment_date": 'datestr',
    "agreed_delivery_lcoation_id": '4'
},  {"sales_order_id": 1,
    "created_at": '2025-08-01 12-12-12.000000',
    "last_updated": '2024-08-01 12-12-12.000000',
    "design_id": 1, 
    "staff_id": 2,
    "counterparty_id": 3,
    "units_sold": 2,
    "unit_price": 55,
    "currency_id": 889,
    "agreed_delivery_date": 'datestr',
    "agreed_payment_date": 'datestr',
    "agreed_delivery_lcoation_id": '4'}]

expected_output = [{'created_date': '2025-09-01',
                    'created_time': '12-12-12.000000',
                    'last_updated_date': '2024-09-01',
                    'last_updated_time': '12-12-12.000000',
                    'design_id': 1, 'staff_id': 2,
                    'counterparty_id': 3, 'units_sold': 2,
                    'unit_price': 3, 'currency_id': 3,
                    'agreed_delivery_date': 'datestr',
                    'agreed_payment_date': 'datestr',
                    'agreed_delivery_lcoation_id': '4'}, 
                    {'created_date': '2025-08-01',
                     'created_time': '12-12-12.000000',
                     'last_updated_date': '2024-08-01',
                     'last_updated_time': '12-12-12.000000',
                     'design_id': 1, 'staff_id': 2,
                     'counterparty_id': 3, 'units_sold': 2,
                     'unit_price': 55, 'currency_id': 889,
                     'agreed_delivery_date': 'datestr',
                     'agreed_payment_date': 'datestr',
                     'agreed_delivery_lcoation_id': '4'}]

def test_list_processed_correctly():
    assert get_fact_sales_order(test_list)== expected_output
