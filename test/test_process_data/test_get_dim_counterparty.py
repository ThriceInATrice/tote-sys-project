import os
import json
from src.process_data.get_dim_counterparty import get_dim_counterparty

DB_CREDENTIALS_ID = os.getenv("DB_CREDENTIALS_ID")


with open('test/test_process_data/test_data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)['data']

test_counterparty = data['counterparty']
test_address = data['address']

def test_func_returns_list():
    print(DB_CREDENTIALS_ID)
    assert isinstance(get_dim_counterparty(DB_CREDENTIALS_ID, test_counterparty), list)

def test_list_contains_items():
    result = get_dim_counterparty(DB_CREDENTIALS_ID, test_counterparty)
    assert len(result) > 0

def test_list_contains_dict():
    result = get_dim_counterparty(DB_CREDENTIALS_ID, test_counterparty)
    assert all([isinstance(item, dict) for item in result])