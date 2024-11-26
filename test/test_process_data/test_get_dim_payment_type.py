from src.process_data.get_dim_payment_type import get_dim_payment_type
import json


with open("test/test_process_data/test_data.json", "r", encoding="utf-8") as f:
    data = json.load(f)["data"]["payment_type"]


def test_func_returns_list():
    assert isinstance(get_dim_payment_type(data), list)


def test_list_contains_items():
    result = get_dim_payment_type(data)
    assert len(result) > 0


def test_list_contains_dict():
    result = get_dim_payment_type(data)
    assert all([isinstance(item, dict) for item in result])


def test_if_payment_type_id_is_key_in_dict():
    result = get_dim_payment_type(data)
    assert all(["payment_type_id" in item for item in result])


def test_if_payment_type_name_is_key_in_dict():
    result = get_dim_payment_type(data)
    assert all(["payment_type_name" in item for item in result])


def test_key_value_is_correct_type():
    result = get_dim_payment_type(data)
    assert all([isinstance(item["payment_type_id"], str) for item in result])
    assert all([isinstance(item["payment_type_name"], str) for item in result])


def test_function_processes_data_correctly():
    test_data = [
        {
            "payment_type_id": "1",
            "payment_type_name": "SALES_RECEIPT",
            "created_at": "2022-11-03 14:20:49.962000",
            "last_updated": "2022-11-03 14:20:49.962000",
        }
    ]
    expected_return = [{"payment_type_id": "1", "payment_type_name": "SALES_RECEIPT"}]
    assert get_dim_payment_type(test_data) == expected_return
