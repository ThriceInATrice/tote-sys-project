from src.get_new_data_from_database import get_new_data_from_database
from unittest.mock import patch
from moto import mock_aws
import pytest


test_cred_dict = {
    'host': 'test_host',
    'port': 5432,
    'user': 'test_user',
    'database': 'test_database',
    'password': 'test_password'
    }


'''
this is a fixure which patches the test cred dict as the 
return value of the get_database_creds func so that 
connect_to_db func can be passed it. psycopg2's connect 
method is then patched so it returns an empty cursor to 
be passed into the get_new_data_from_database funtion in 
the tests below.
'''


@pytest.fixture(autouse=True)
def patch_psycopg2_conn_and_creds():

    with patch ('src.connection.get_database_creds') as patched_creds:
        patched_creds.return_value = test_cred_dict

        with patch('psycopg2.connect') as patched_psycopg2:
            yield patched_psycopg2


@mock_aws
def test(patch_psycopg2_conn_and_creds):   
    result = get_new_data_from_database(patch_psycopg2_conn_and_creds)
    print(result)