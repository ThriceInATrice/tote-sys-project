from src.extraction.extract import lambda_handler
from src.extraction.get_new_data_from_database import get_new_data_from_database
from unittest.mock import patch
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
def test_extract_works_correctly(test_data_from_test_database):
    # create ingestion_bucket and extraciton_times bucket
    ingestion_bucket_name = "ingestion_bucket"
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=ingestion_bucket_name)

    extraction_bucket_name = "extraction_times_bucket"
    body = json.dumps({"extraction_times": []})
    s3_client.create_bucket(Bucket=extraction_bucket_name)
    s3_client.put_object(
        Bucket=extraction_bucket_name, Key="extraction_times.json", Body=body
    )

    # set up event, context can be empty
    # secret id needs to be present but it doesnt matter what it is as get_database_creds is being patched
    event = {
        "credentials_id": "secret_id",
        "ingestion_bucket": ingestion_bucket_name,
        "extraction_times_bucket": extraction_bucket_name,
    }
    context = {}

    # run extract
    extraction_time = lambda_handler(event, context)

    date_split = re.findall("[0-9]+", extraction_time)
    ingestion_key = "/".join(
        [date_split[0], date_split[1], date_split[2], extraction_time + ".json"]
    )

    # check data is in ingestion_bucket
    expected_ingestion_content = {
        "extraction_time": extraction_time,
        "data": {
            "test_table": [
                {
                    "test_id": "1",
                    "test_text_1": "A",
                    "test_text_2": "a",
                    "test_bool": "True",
                },
                {
                    "test_id": "2",
                    "test_text_1": "B",
                    "test_text_2": "b",
                    "test_bool": "False",
                },
                {
                    "test_id": "3",
                    "test_text_1": "C",
                    "test_text_2": "c",
                    "test_bool": "True",
                },
                {
                    "test_id": "4",
                    "test_text_1": "D",
                    "test_text_2": "d",
                    "test_bool": "False",
                },
                {
                    "test_id": "5",
                    "test_text_1": "E",
                    "test_text_2": "e",
                    "test_bool": "True",
                },
                {
                    "test_id": "6",
                    "test_text_1": "F",
                    "test_text_2": "f",
                    "test_bool": "False",
                },
            ]
        },
    }

    response = s3_client.get_object(Bucket=ingestion_bucket_name, Key=ingestion_key)
    ingestion_body = response["Body"]
    ingestion_bytes = ingestion_body.read()
    data = json.loads(ingestion_bytes)
    
    assert data == expected_ingestion_content

    # check extraction time is in extraction_times_bucket
    response = s3_client.get_object(
        Bucket=extraction_bucket_name, Key="extraction_times.json"
    )
    extraction_body = response["Body"]
    extraction_bytes = extraction_body.read()
    extraction_dict = json.loads(extraction_bytes)
    extraction_list = extraction_dict["extraction_times"]
    
    assert extraction_list[-1] == extraction_time
