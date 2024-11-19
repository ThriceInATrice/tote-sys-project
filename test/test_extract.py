from src.extraction.extract import lambda_handler
from moto import mock_aws
from configparser import ConfigParser
import boto3
import json
import psycopg2
import pytest
import re

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


@mock_aws
def test_extract_works_correctly():
    # make a mock rds with boto3
    rds_client = boto3.client("rds", region_name="eu-west-2")
    db_name = "test-db"
    db_user = "test-db-user"
    db_password = "test-db-password"

    rds_response = rds_client.create_db_instance(
        DBInstanceIdentifier=db_name,
        DBInstanceClass="db.t2.micro",
        Engine="postgres",
        MasterUsername=db_user,
        MasterUserPassword=db_password,
    )

    db_endpoint = rds_response["DBInstance"]["Endpoint"]["Address"]
    db_port = rds_response["DBInstance"]["Endpoint"]["Port"]
    print(db_endpoint)

    # set up test secret with rds details
    secret_client = boto3.client("secretsmanager", "eu-west-2")
    secret_id = "test_credentials"
    test_credentials = {
        "database": db_name,
        "user": db_user,
        "password": db_password,
        "host": db_endpoint,
        "port": db_port,
    }

    secret_string = json.dumps(test_credentials)
    secret_client.create_secret(Name=secret_id, SecretString=secret_string)

    # create ingestion_bucket and extraciton_times bucket
    ingestion_bucket_name = "ingestion_bucket"
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=ingestion_bucket_name)

    extraction_bucket_name = "extraction_times_bucket"
    body = json.dumps({"extraction_times": []})
    s3_client.create_bucket(Bucket=extraction_bucket_name)
    s3_client.put_object(
        Bucket=extraction_bucket_name, Key="extraction_times", Body=body
    )

    # set up event, context can be empty
    event = {
        "credentials_id": secret_id,
        "ingestion_bucket": ingestion_bucket_name,
        "extraction_times_bucket": extraction_bucket_name,
    }
    context = {}

    # run extract
    lambda_handler(event, context)

    # check data is in ingestion_bucket
    response = s3_client.get_object(
        Bucket=ingestion_bucket_name, Key="2024/11/1/2024.11.1.14.30.1.10.json"
    )
    ingestion_body = response["Body"]
    ingestion_bytes = ingestion_body.read()
    data = json.loads(ingestion_bytes)

    assert data == expected_ingestion_content

    # check extraction time is in extraction_times_bucket
    response = s3_client.get_object(
        Bucket=extraction_bucket_name, key="extraction_times"
    )
    extraction_body = response["Body"]
    extraction_bytes = extraction_body.read()
    extraction_dict = json.loads(extraction_bytes)
    extraction_list = extraction_dict["extraction_times"]

    assert extraction_list[-1] == extraction_time
