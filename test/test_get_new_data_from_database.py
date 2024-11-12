from src.get_new_data_from_database import get_new_data_from_database
from test_database.test_config import load_test_config
from unittest.mock import patch
from moto import mock_aws
import pytest
import psycopg2

config = load_test_config()

test_cred_dict = {
    'host': config['host'],
    'port': config['port'],
    'user': config['user'],
    'database': config['database'],
    'password': config['password']
    }


'''
this is a fixure which patches the test cred dict as the 
return value of the get_database_creds func so that 
connect_to_db func can be passed it. psycopg2's connect 
method is then patched so it returns an empty cursor to 
be passed into the get_new_data_from_database funtion in 
the tests below.
'''



def connect():
    try:
        conn = psycopg2.connect(**config)
        return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
        
    
def test():
    with patch('src.connection.get_database_creds') as patched_creds:
        patched_creds.return_value = test_cred_dict

        config = load_test_config()

        try:
            with psycopg2.connect(**config) as conn:
                cursor = conn.cursor()

                with patch('src.connection.connect_to_db') as patched_conn:
                    patched_conn.return_vale = cursor
                    
                    print(get_new_data_from_database(''))

        except (psycopg2.DatabaseError, Exception) as error:
            print(error)
    



# @pytest.fixture(autouse=True)
# def patch_psycopg2_conn_and_creds():

#     with patch ('src.connection.get_database_creds') as patched_creds:
#         patched_creds.return_value = test_cred_dict

#         with patch('psycopg2.connect') as patched_psycopg2:
#             yield connect()

# def test_returns_a_dict(patch_psycopg2_conn_and_creds):
#     result = get_new_data_from_database(patch_psycopg2_conn_and_creds)
#     assert type(result) is dict

# def test_terminal_logs_how_many_tables_there_are(patch_psycopg2_conn_and_creds, capsys):
#     expected = 'there are 0 and they are []'
#     get_new_data_from_database(patch_psycopg2_conn_and_creds)

#     captured = capsys.readouterr().out.strip()
#     assert captured == expected