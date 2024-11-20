from pprint import pprint
import json
from src.process_data.get_dim_counterparty import get_dim_counterparty


with open('test/test_process_data/test_data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)['data']

test_counterparty = data['counterparty']
test_address = data['address']

def test_func_returns_list():
    result = get_dim_counterparty(test_counterparty, test_address)
    pprint(result)
    assert isinstance(result, list)

def test_list_contains_items():
    result = get_dim_counterparty(test_counterparty, test_address)
    assert len(result) > 0

def test_list_contains_dict():
    result = get_dim_counterparty(test_counterparty, test_address)
    assert all([isinstance(item, dict) for item in result])