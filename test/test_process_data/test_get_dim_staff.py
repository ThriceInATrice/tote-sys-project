from src.process_data.get_dim_staff import (
    get_dim_staff,
    DBCredentialsExportError,
    UnexpectedDimStaffError,
)
import os
import pytest
import re

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


def test_process_staff_returns_correct_data_types_for_single_dict_values():
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
    result = get_dim_staff(DB_CREDENTIALS_ID, input_list)
    assert type(result[0]["staff_id"]) == int
    assert type(result[0]["first_name"]) == str
    assert type(result[0]["last_name"]) == str
    assert type(result[0]["department_name"]) == str
    assert type(result[0]["location"]) == str
    assert type(result[0]["email_address"]) == str


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


def test_process_staff_returns_correct_data_types_for_multiple_dicts_values():
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

    result = get_dim_staff(DB_CREDENTIALS_ID, input_list)
    for row in result:
        assert type(row["staff_id"]) == int
        assert type(row["first_name"]) == str
        assert type(row["last_name"]) == str
        assert type(row["department_name"]) == str
        assert type(row["location"]) == str
        assert type(row["email_address"]) == str


def test_get_dim_staff_raises_error_when_db_credentials_not_exported():
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
    with pytest.raises(
        DBCredentialsExportError,
        match=re.escape(
            "Please enter export DB_CREDENTIALS_ID=[Your database credentials ID] into terminal."
        ),
    ):
        get_dim_staff(None, input_list)


def test_get_dim_staff_raises_error_with_incorrect_db_credentials_exported():
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
    with pytest.raises(
        DBCredentialsExportError,
        match=re.escape(
            "Incorrect Credentials ID: Please export DB_CREDENTIALS_ID=[Your database credentials ID] into terminal."
        ),
    ):
        get_dim_staff("incorrect_creds", input_list)


def test_get_dim_staff_raises_error_if_not_list_type_input():
    input_list = 3
    with pytest.raises(TypeError, match="Input must be a list"):
        get_dim_staff(DB_CREDENTIALS_ID, input_list)


def test_get_dim_staff_raises_error_if_list_contains_non_dictionary_types():
    input_list = [1, {"test": 1}, 2]
    with pytest.raises(TypeError, match="Input must be a list of dictionaries"):
        get_dim_staff(DB_CREDENTIALS_ID, input_list)
