from src.extraction.extract import log_extraction_time
from moto import mock_aws
import boto3, json


def test_extract_works_correctly():
    # set up test secret
    client = boto3.client("secretsmanager", "eu-west-2")
    secret_id = "test_credentials"
    test_credentials = {"credentials": "test"}
    secret_string = json.dumps(test_credentials)
    client.create_secret(Name=secret_id, SecretString=secret_string)

    # set up extraction_times bucket
    bucket_name = "extraction_times_bucket"
    body = json.dumps({"extraction_times": []})
    client = boto3.client("s3")
    client.create_bucket(Bucket=bucket_name)
    client.put_object(Bucket=bucket_name, Key="extraction_times", Body=body)

    # patch get_new_data
