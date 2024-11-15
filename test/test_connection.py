from moto import mock_aws
import boto3, json, pytest
from src.extraction.connection import get_database_creds
from src.extraction.ingestion_error import IngestionError
from botocore.exceptions import ClientError


@mock_aws
class TestGetDatabaseCreds:
    def test_get_database_creds_can_return_creds(self):
        client = boto3.client("secretsmanager", "eu-west-2")
        secret_id = "test_credentials"
        test_credentials = {"credentials": "test"}
        secret_string = json.dumps(test_credentials)
        client.create_secret(Name=secret_id, SecretString=secret_string)
        assert get_database_creds(secret_id) == test_credentials

    def test_get_database_creds_raises_error_when_no_object_matches_credentials_id(
        self,
    ):
        secret_id = "test_credentials"

        with pytest.raises(TypeError):
            get_database_creds(secret_id)
