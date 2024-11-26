from configparser import ConfigParser
from pprint import pprint
import json

from moto import mock_aws
import boto3
import pytest

from src.load.load_data_handler import lambda_handler as handler
from src.load.connection import connect_to_db

with open ('test/test_load/mock_processed_data.json', 'r', encoding='utf-8') as f:
    mock_data = json.load(f)['data']

processed_bucket_name = 'processed_data_bucket'
loaded_extractions_bucket_name = "loaded_extractions_bucket"
object_key = 'processed_data.json'
object_body = mock_data


handler_args = [{ 
            "warehouse_credentials_id": "test_dw_creds",
    'processed_data_bucket': processed_bucket_name,
    'loaded_extractions_bucket': loaded_extractions_bucket_name
        }, {}]

parser = ConfigParser()
parser.read("test/test_load/test_load_database.ini")
params = parser.items("postgresql_test_load_database")
config_dict = {param[0]: param[1] for param in params}


@mock_aws
class TestLambdaHandler:
    def test_lambda_handler_processes_data_correctly(self):

        # create mock secrets manager and store datawarehouse credentials
        secrets_client = boto3.client("secretsmanager", region_name="eu-west-2")
        secrets_client.create_secret(Name='test_dw_creds', SecretString=str(json.dumps(config_dict)))


        #create mock s3 bucket to store mock processed data jsons
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.create_bucket(Bucket=loaded_extractions_bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        s3.create_bucket(Bucket=processed_bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        s3.put_object(
            Bucket=processed_bucket_name,
            Key=object_key,
            Body=json.dumps(object_body),
        )

        handler(*handler_args)