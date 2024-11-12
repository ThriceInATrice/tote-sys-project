from src.get_new_data_from_database import get_new_data_from_database
from test_database.test_config import load_test_config
from unittest.mock import patch
from moto import mock_aws
import psycopg2
import pytest

config = load_test_config()

@pytest.fixture(autouse=True)
def test_data_from_test_database():
    with patch('src.connection.get_database_creds') as patched_creds:
        patched_creds.return_value = config
        with psycopg2.connect(**config) as conn:
            with patch('src.connection.connect_to_db', conn.cursor):                    
                yield(get_new_data_from_database(credentials_id=None))

        # except (psycopg2.DatabaseError, Exception) as error:
        #     print(error)
    
# @pytest.fixture(autouse=True)
# def patch_psycopg2_conn_and_creds():

#     with patch ('src.connection.get_database_creds') as patched_creds:
#         patched_creds.return_value = test_cred_dict

#         with patch('psycopg2.connect') as patched_psycopg2:
#             yield connect()

def test_returns_a_dict(test_data_from_test_database):
    result = test_data_from_test_database
    assert isinstance(result, dict)
    
def test_terminal_logs_how_many_tables_there_are(capsys):
    expected = "there are 1 and they are ['test_table']"
    get_new_data_from_database(credentials_id=None)
    
    captured = capsys.readouterr().out.strip()
    assert captured == expected