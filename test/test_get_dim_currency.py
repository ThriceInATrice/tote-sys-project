from src.process_data.get_dim_currency import get_dim_currency, get_currency_name
import pytest
from src.process_data.processing_error import ProcessingError


class TestGetCurrencyName:
    def test_get_currency_returns_string(self):
        assert isinstance(get_currency_name("gbp"), str)

    def test_get_currency_returns_correct_name_for_common_currencies(self):
        assert get_currency_name("gbp") == "British Pound"
        assert get_currency_name("usd") == "US Dollar"
        assert get_currency_name("grd") == "Greek Drachma"

    def test_get_dim_currency_raises_error_correctly_with_unknown_code(self):
        with pytest.raises(ProcessingError):
            get_currency_name("not_a_currency_code")


class TestGetDimCurrency:
    def func_returns_empty_list_when_given_empty_list(self):
        assert get_dim_currency([]) == []

    def test_func_returns_list_of_dicts(self):
        test_currency_list = [
            {
                "currency_id": 1,
                "currency_code": "gbp",
                "created_at": "yesterday",
                "last_updated": "today",
            }
        ]
        result = get_dim_currency(test_currency_list)
        assert isinstance(result, list)
        assert all([isinstance(item, dict) for item in result])

    def test_get_dim_currency_processes_one_entry_correctly(self):
        test_currency_list = [
            {
                "currency_id": 1,
                "currency_code": "gbp",
                "created_at": "yesterday",
                "last_updated": "today",
            }
        ]
        expected_return = [
            {"currency_id": 1, "currency_code": "gbp", "currency_name": "British Pound"}
        ]
        assert get_dim_currency(test_currency_list) == expected_return

    def test_get_dim_currency_can_handle_multiple_entries(self):
        test_currency_list = [
            {
                "currency_id": 1,
                "currency_code": "gbp",
                "created_at": "yesterday",
                "last_updated": "today",
            },
            {
                "currency_id": 2,
                "currency_code": "usd",
                "created_at": "yesterday",
                "last_updated": "today",
            },
        ]
        expected_return = [
            {
                "currency_id": 1,
                "currency_code": "gbp",
                "currency_name": "British Pound",
            },
            {"currency_id": 2, "currency_code": "usd", "currency_name": "US Dollar"},
        ]
        assert get_dim_currency(test_currency_list) == expected_return

    def test_det_dim_currency_raises_error_correctly_when_given_invalid_currency_code(
        self,
    ):
        test_currency_list = [
            {
                "currency_id": 1,
                "currency_code": "not_a_currnecy_code",
                "created_at": "yesterday",
                "last_updated": "today",
            }
        ]
        with pytest.raises(ProcessingError):
            get_dim_currency(test_currency_list)
