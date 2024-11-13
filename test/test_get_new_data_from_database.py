from src.get_new_data_from_database import get_new_data_from_database
from src.connection import get_database_creds
from configparser import ConfigParser
from unittest.mock import patch
from moto import mock_aws
import psycopg2
import pytest
import boto3
import json


@mock_aws
def test_get_database_creds_can_return_creds():
    client = boto3.client("secretsmanager", "eu-west-2")
    secret_id = "test_credentials"
    test_credentials = {"credentials": "test"}
    secret_string = json.dumps(test_credentials)
    client.create_secret(Name=secret_id, SecretString=secret_string)
    assert get_database_creds(secret_id) == test_credentials

def get_new_data_from_database_gets_all_data_when_last_updated_is_falsy():
    pass

def get_new_data_from_database_gets_correct_data_when_last_updated_valid():
    pass


#tests with dummy database and patched credentials

@pytest.fixture()
def test_data_from_test_database():

    parser = ConfigParser()
    parser.read('test/test_database.ini')
    params = parser.items('postgresql_test_database')
    config_dict = {param[0]: param[1] for param in params}

    with patch('src.connection.get_database_creds') as patched_creds:
        patched_creds.return_value = config_dict
        with psycopg2.connect() as conn:
            with patch('src.connection.connect_to_db', conn.cursor):                    
                yield(get_new_data_from_database(credentials_id=None))

def test_returns_a_dict(test_data_from_test_database):
    result = test_data_from_test_database
    assert isinstance(result, dict)
    
def test_terminal_logs_how_many_tables_there_are(test_data_from_test_database, capsys):
    expected = "there are 1 and they are ['test_table']"
    get_new_data_from_database(credentials_id=None)
    
    captured = capsys.readouterr().out.strip()
    assert captured == expected

def test_get_new_data_from_database_returns_data_from_database(test_data_from_test_database):
    result = test_data_from_test_database
    result_value = [value for key, value in result.items()][0][0]
    expected = (1, 'A', 'a', True)
    assert result_value[0] == expected