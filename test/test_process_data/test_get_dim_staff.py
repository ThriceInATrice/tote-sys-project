from src.process_data.get_dim_staff import get_dim_staff
import os

DB_CREDENTIALS_ID = os.getenv("DB_CREDENTIALS_ID")


def test_process_staff_returns_correct_data_for_single_dict():
    input_list = [
        {
            "staff_id": 1,
            "first_name": "Jeremie",
            "last_name": "Franey",
            "department_id": 2,
            "email_address": "jeremie.franey@terrifictotes.com",
            "created_at": "2022-11-03 14:20:51.563000",
            "last_updated": "2022-11-03 14:20:51.563000",
        }
    ]
    expected_list = [
        {
            "staff_id": 1,
            "first_name": "Jeremie",
            "last_name": "Franey",
            "department_name": "Purchasing",
            "location": "Manchester",
            "email_address": "jeremie.franey@terrifictotes.com",
        }
    ]
    result = get_dim_staff(DB_CREDENTIALS_ID, input_list)
    assert result == expected_list


def test_process_staff_returns_correct_data_for_multiple_dicts():
    input_list = [
        {
            "staff_id": 1,
            "first_name": "Jeremie",
            "last_name": "Franey",
            "department_id": 2,
            "email_address": "jeremie.franey@terrifictotes.com",
            "created_at": "2022-11-03 14:20:51.563000",
            "last_updated": "2022-11-03 14:20:51.563000",
        },
        {
            "staff_id": 2,
            "first_name": "Deron",
            "last_name": "Beier",
            "department_id": 6,
            "email_address": "deron.beier@terrifictotes.com",
            "created_at": "2022-11-03 14:20:51.563000",
            "last_updated": "2022-11-03 14:20:51.563000",
        },
    ]

    expected_list = [
        {
            "staff_id": 1,
            "first_name": "Jeremie",
            "last_name": "Franey",
            "department_name": "Purchasing",
            "location": "Manchester",
            "email_address": "jeremie.franey@terrifictotes.com",
        },
        {
            "staff_id": 2,
            "first_name": "Deron",
            "last_name": "Beier",
            "department_name": "Facilities",
            "location": "Manchester",
            "email_address": "deron.beier@terrifictotes.com",
        },
    ]
    result = get_dim_staff(DB_CREDENTIALS_ID, input_list)
    assert result == expected_list
