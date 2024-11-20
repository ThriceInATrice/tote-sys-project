from src.extraction.get_new_data_from_database import get_new_data_from_database
from configparser import ConfigParser
from unittest.mock import patch
from moto import mock_aws
import psycopg2, pytest
from pprint import pprint

full_expected_db = [
    {"test_id": "1", "test_text_1": "A", "test_text_2": "a", "test_bool": "True"},
    {"test_id": "2", "test_text_1": "B", "test_text_2": "b", "test_bool": "False"},
    {"test_id": "3", "test_text_1": "C", "test_text_2": "c", "test_bool": "True"},
    {"test_id": "4", "test_text_1": "D", "test_text_2": "d", "test_bool": "False"},
    {"test_id": "5", "test_text_1": "E", "test_text_2": "e", "test_bool": "True"},
    {"test_id": "6", "test_text_1": "F", "test_text_2": "f", "test_bool": "False"},
]

parser = ConfigParser()
parser.read("test/test_database.ini")
params = parser.items("postgresql_test_database")
config_dict = {param[0]: param[1] for param in params}



@pytest.fixture()
def test_data_from_test_database():
    with patch("src.extraction.connection.get_database_creds") as patched_creds:
        patched_creds.return_value = config_dict
        with psycopg2.connect() as conn:
            with patch("src.extraction.connection.connect_to_db", conn.cursor):
                yield (get_new_data_from_database(credentials_id=None))


def test_returns_a_dict(test_data_from_test_database):
    result = test_data_from_test_database
    assert isinstance(result, dict)


def test_get_new_data_from_database_returns_data_from_database_as_dict_with_correct_column_name_as_key(
    test_data_from_test_database,
):
    result = test_data_from_test_database
    print(result)
    expected = {
        "test_id": "1",
        "test_text_1": "A",
        "test_text_2": "a",
        "test_bool": "True",
    }
    assert result["data"]["test_table"][0] == expected



def test_get_new_data_returns_every_row_in_database(test_data_from_test_database):
    result = test_data_from_test_database
    result_value = result["data"]["test_table"]
    assert all(
        [result_value[i] == full_expected_db[i] for i in range(len(full_expected_db))]
    )


def test_get_new_data_from_database_gets_all_data_when_last_updated_is_falsy(
    test_data_from_test_database,
):
    result = get_new_data_from_database(credentials_id=None, last_extraction=None)
    result_value = result["data"]["test_table"]
    assert result_value == full_expected_db


# def test_get_new_data_from_database_gets_correct_data_when_last_updated_valid(test_data_from_test_database):
#     result = get_new_data_from_database(credentials_id=None, last_update=datetime(2024, 11, 13, 14, 9, 40, 33221))
#     print(result)
