import os
import json
import pytest
from src.process_data.get_dim_counterparty import get_dim_counterparty
from src.process_data.processing_error import ProcessingError

DB_CREDENTIALS_ID = os.getenv("DB_CREDENTIALS_ID")


with open("test/test_process_data/test_data.json", "r", encoding="utf-8") as f:
    data = json.load(f)["data"]

test_counterparty = data["counterparty"]
test_address = data["address"]


def test_func_returns_list_of_dictionaries():
    result = get_dim_counterparty(DB_CREDENTIALS_ID, test_counterparty)
    assert isinstance(result, list)
    assert all([isinstance(item, dict) for item in result])


def test_func_processes_data_correctly():
    test_data = [
        {
            "counterparty_id": "1",
            "counterparty_legal_name": "Fahey and Sons",
            "legal_address_id": "15",
            "commercial_contact": "Micheal Toy",
            "delivery_contact": "Mrs. Lucy Runolfsdottir",
            "created_at": "2022-11-03 14:20:51.563000",
            "last_updated": "2022-11-03 14:20:51.563000",
        },
        {
            "counterparty_id": "2",
            "counterparty_legal_name": "Leannon, Predovic and Morar",
            "legal_address_id": "28",
            "commercial_contact": "Melba Sanford",
            "delivery_contact": "Jean Hane III",
            "created_at": "2022-11-03 14:20:51.563000",
            "last_updated": "2022-11-03 14:20:51.563000",
        },
    ]
    expected_return = [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "Fahey and Sons",
            "counterparty_legal_address_line_1": "605 Haskell Trafficway",
            "counterparty_legal_address_line_2": "Axel Freeway",
            "counterparty_legal_district": None,
            "counterparty_legal_city": "East Bobbie",
            "counterparty_legal_postal_code": "88253-4257",
            "counterparty_legal_country": "Heard Island and McDonald Islands",
            "counterparty_legal_phone_number": "9687 937447",
        },
        {
            "counterparty_id": 2,
            "counterparty_legal_name": "Leannon, Predovic and Morar",
            "counterparty_legal_address_line_1": "079 Horacio Landing",
            "counterparty_legal_address_line_2": None,
            "counterparty_legal_district": None,
            "counterparty_legal_city": "Utica",
            "counterparty_legal_postal_code": "93045",
            "counterparty_legal_country": "Austria",
            "counterparty_legal_phone_number": "7772 084705",
        },
    ]

    assert get_dim_counterparty(DB_CREDENTIALS_ID, test_data) == expected_return


def test_func_raises_error_correctly_when_connection_fails():
    with pytest.raises(ProcessingError):
        get_dim_counterparty("wrong credentials", [])
