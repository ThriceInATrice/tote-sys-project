from moto import mock_aws
import boto3
import json
from src.connection import get_database_creds, connect_to_db


@mock_aws
def test_get_database_creds_can_return_creds():
    client = boto3.client("secretsmanager", "eu-west-2")
    secret_id = "test_credentials"
    test_credentials = {"credentials": "test"}
    secret_string = json.dumps(test_credentials)
    client.create_secret(Name=secret_id, SecretString=secret_string)
    assert get_database_creds(secret_id) == test_credentials


def test_get_tables_finds_table_names_correctly():
    pass


def get_new_data_from_database_gets_all_data_when_last_updated_is_falsy():
    pass


def get_new_data_from_database_gets_correct_data_when_last_updated_valid():
    pass
