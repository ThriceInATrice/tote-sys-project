from configparser import ConfigParser
from pprint import pprint
import json
import psycopg2

from moto import mock_aws
import boto3
import pytest

from src.load.load_data_handler import lambda_handler as handler, get_unloaded_data
from src.load.load_error import LoadError
from src.load.connection import connect_to_db


parser = ConfigParser()
parser.read("test/test_load/test_load_database.ini")
params = parser.items("postgresql_test_load_database")
config_dict = {param[0]: param[1] for param in params}


@pytest.mark.run
@mock_aws
class TestLambdaHandler:
    def test_lambda_handler_processes_data_correctly(self):
        # the expected return from the test database
        expected_staff_data = [(1, 'Jeremie', 'Franey', 'Purchasing', 'Manchester', 'jeremie.franey@terrifictotes.com'), (2, 'Deron', 'Beier', 'Sales', 'Manchester', 'deron.beier@terrifictotes.com')]
        
        
        # create mock secrets manager and store datawarehouse credentials
        secrets_client = boto3.client("secretsmanager", region_name="eu-west-2")
        secrets_client.create_secret(
            Name="test_db_creds", SecretString=str(json.dumps(config_dict))
        )

        # create s3 client
        s3 = boto3.client("s3", region_name="eu-west-2")

        # create mock s3 bucket to store processed extraction times and populate with a single extraction time
        processed_extractions_bucket_name = "extraction_times"
        s3.create_bucket(
            Bucket=processed_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.put_object(
            Bucket=processed_extractions_bucket_name,
            Key="processed_extractions.json",
            Body=json.dumps({"extraction_times": ["2024-11-27"]}),
        )

        # create mock s3 bucket to store extraction load times and populate with a single extraction time
        loaded_extractions_bucket_name = "loaded_extractions_bucket"
        s3.create_bucket(
            Bucket=loaded_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.put_object(
            Bucket=loaded_extractions_bucket_name,
            Key="loaded_extractions.json",
            Body=json.dumps({"extraction_times": []}),
        )

        # create mock s3 bucket to store mock processed data jsons
        processed_bucket_name = "processed_data_bucket"
        s3.create_bucket(
            Bucket=processed_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        with open(
            "test/test_load/mock_processed_data.json", "r", encoding="utf-8"
        ) as f:
            mock_data = json.load(f)
            s3.put_object(
                Bucket=processed_bucket_name,
                Key="2024/11/27/2024-11-27.json",
                Body=json.dumps(mock_data),
            )

        test_event = {
            "warehouse_credentials_id": "test_db_creds",
            "processed_data_bucket": processed_bucket_name,
            "loaded_extractions_bucket": loaded_extractions_bucket_name,
            "processed_extractions_bucket": processed_extractions_bucket_name,
        }

        handler(test_event, {})

        with connect_to_db("test_db_creds") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM dim_staff;")
            database_staff_data = cursor.fetchall()

        assert database_staff_data == expected_staff_data
@mock_aws
class TestGetUnloadedData:

    def test_function_returns_times_that_have_not_been_processed(self):
        # create s3 client
        client = boto3.client("s3")

        # create and populate processed_extractions bucket
        processed_extractions_bucket_name = "processed_extractions_bucket"
        processed_extractions_key = "processed_extractions.json"
        processed_extractions_body = json.dumps({"extraction_times": ["today"]})
        client.create_bucket(
            Bucket=processed_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=processed_extractions_bucket_name,
            Key=processed_extractions_key,
            Body=processed_extractions_body,
        )

        # create and populate loaded_extractions bucket
        loaded_extractions_bucket_name = "loaded_extractions_bucket"
        loaded_extractions_key = "loaded_extractions.json"
        loaded_extractions_body = json.dumps({"extraction_times": []})
        client.create_bucket(
            Bucket=loaded_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=loaded_extractions_bucket_name,
            Key=loaded_extractions_key,
            Body=loaded_extractions_body,
        )

        # event contains relevant information for the function to opperate
        event = {
            "warehouse_credentials_id": "test_db_creds",
            "loaded_extractions_bucket": "loaded_extractions_bucket",
            "processed_data_bucket": "processed_data_bucket",
            "processed_extractions_bucket": "processed_extractions_bucket",
        }

        # assert that the function returns the string from extraction_times
        assert get_unloaded_data(event) == ["today"]

    def test_function_returns_empty_list_when_all_times_have_been_processed(self):
        # create s3 client
        client = boto3.client("s3")

        # create and populate processed_extractions bucket
        processed_extractions_bucket_name = "processed_extractions_bucket"
        processed_extractions_key = "processed_extractions.json"
        processed_extractions_body = json.dumps({"extraction_times": ["today"]})
        client.create_bucket(
            Bucket=processed_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=processed_extractions_bucket_name,
            Key=processed_extractions_key,
            Body=processed_extractions_body,
        )

        # create and populate loaded_extractions bucket
        loaded_extractions_bucket_name = "loaded_extractions_bucket"
        loaded_extractions_key = "loaded_extractions.json"
        loaded_extractions_body = json.dumps({"extraction_times": ["today"]})
        client.create_bucket(
            Bucket=loaded_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=loaded_extractions_bucket_name,
            Key=loaded_extractions_key,
            Body=loaded_extractions_body,
        )

        # event contains relevant information for the function to opperate
        event = {
            "warehouse_credentials_id": "test_db_creds",
            "loaded_extractions_bucket": "loaded_extractions_bucket",
            "processed_data_bucket": "processed_data_bucket",
            "processed_extractions_bucket": "processed_extractions_bucket",
        }

        assert get_unloaded_data(event) == []

    def test_function_creates_empty_json_if_processed_extractions_bucket_is_empty(self):
        # create s3 client
        client = boto3.client("s3")

        # create and populate processed_extractions bucket
        processed_extractions_bucket_name = "processed_extractions_bucket"
        processed_extractions_key = "processed_extractions.json"
        processed_extractions_body = json.dumps({"extraction_times": ["today"]})
        client.create_bucket(
            Bucket=processed_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=processed_extractions_bucket_name,
            Key=processed_extractions_key,
            Body=processed_extractions_body,
        )

        # create and populate loaded_extractions bucket
        loaded_extractions_bucket_name = "loaded_extractions_bucket"
        client.create_bucket(
            Bucket=loaded_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # event contains relevant information for the function to opperate
        event = {
            "warehouse_credentials_id": "test_db_creds",
            "loaded_extractions_bucket": "loaded_extractions_bucket",
            "processed_data_bucket": "processed_data_bucket",
            "processed_extractions_bucket": "processed_extractions_bucket",
        }

        assert get_unloaded_data(event) == ["today"]
