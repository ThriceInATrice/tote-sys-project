from src.process_data.get_dim_date import get_new_dates, get_dim_date, get_date_object
from datetime import date


class TestGetNewDates:
    def test_get_new_dates_finds_dates_under_relevant_fields(self):
        test_data = {
            "dict_list_1": [
                {"created_date": "date_1"},
                {"last_updated_date": "date_2"},
            ],
            "dict_list_2": [
                {
                    "agreed_delivery_date": "date_3",
                    "agreed_payment_date": "date_4",
                    "payment_date": "date_5",
                }
            ],
        }
        assert get_new_dates(test_data) == [
            "date_1",
            "date_2",
            "date_3",
            "date_4",
            "date_5",
        ]

    def test_get_new_dates_does_not_repreat_dates(self):
        test_data = {
            "dict_list_1": [
                {"created_date": "date_1"},
                {"last_updated_date": "date_2"},
            ],
            "dict_list_2": [
                {
                    "agreed_delivery_date": "date_1",
                    "agreed_payment_date": "date_2",
                    "payment_date": "date_3",
                }
            ],
        }
        assert get_new_dates(test_data) == ["date_1", "date_2", "date_3"]

    def test_get_new_dates_ignores_values_from_non_date_keys(self):
        test_data = {
            "dict_list_1": [
                {"created_date": "date_1"},
                {"staff": "Alice"},
            ],
            "dict_list_2": [
                {
                    "currency": "usd",
                    "agreed_payment_date": "date_2",
                    "some_other_key": "date_3",
                }
            ],
        }
        assert get_new_dates(test_data) == ["date_1", "date_2"]


class TestGetDateObject:
    def test_if_get_date_object_successfully_produces_a_date_object(self):
        expected_date = date(2024, 11, 19)
        test_date_id = "20241119"
        assert get_date_object(test_date_id) == expected_date

    def test_get_date_object_handles_single_digit_month_or_day_numbers(self):
        expected_date = date(2024, 1, 9)
        test_date_id = "20240109"
        assert get_date_object(test_date_id) == expected_date


class TestGetDimDate:
    def test_get_dim_date_returns_list_of_dicts(self):
        test_data = {
            "dict_list_1": [
                {"created_date": "20241119"},
            ]
        }
        result = get_dim_date(test_data)
        assert isinstance(result, list)
        assert all([isinstance(item, dict) for item in result])

    def test_get_dim_date_returns_empty_list_when_input_data_has_no_entries(self):
        test_data = {"dict_list_1": [], "dict_list_2": []}
        assert get_dim_date(test_data) == []

    def test_get_dim_date_returns_correct_date_info(self):
        test_data = {
            "dict_list_1": [
                {"created_date": "20241119"},
            ]
        }
        assert get_dim_date(test_data) == [
            {
                "date_id": 20241119,
                "year": 2024,
                "month": 11,
                "day": 19,
                "day_of_week": 2,
                "day_name": "Tuesday",
                "month_name": "November",
                "quarter": 4,
            }
        ]
