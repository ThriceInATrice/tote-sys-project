from src.extraction.store_new_data import store_new_data
from src.extraction.ingestion_error import IngestionError
from moto import mock_aws
import boto3, json, pytest


@mock_aws
class TestStoreNewData:
    def test_if_func_puts_data_successfully(self):
        bucket_name = "test_bucket"
        extraction_time = "2024.11.1.14.30.1.10"
        data = {"extraction_time": extraction_time, "data": "my_data"}

        client = boto3.client("s3")
        client.create_bucket(Bucket=bucket_name)

        store_new_data(bucket_name, data)

        response = client.get_object(
            Bucket=bucket_name, Key="2024/11/1/2024.11.1.14.30.1.10.json"
        )
        response_body = response["Body"]
        response_bytes = response_body.read()
        response_data = json.loads(response_bytes)

        assert response_data == data

    def test_func_does_not_change_other_data_in_bucket(self):
        bucket_name = "test_bucket"
        extraction_time = "2024.11.1.14.30.1.10"
        data = {"extraction_time": extraction_time, "data": "my_data"}

        other_key = "other key"
        other_body = {"other body": "other_body"}

        client = boto3.client("s3")
        client.create_bucket(Bucket=bucket_name)
        client.put_object(
            Bucket=bucket_name, Key=other_key, Body=json.dumps(other_body)
        )

        store_new_data(bucket_name, data)

        response = client.get_object(Bucket=bucket_name, Key=other_key)
        response_body = response["Body"]
        response_bytes = response_body.read()
        response_data = json.loads(response_bytes)

        assert response_data == other_body

    def test_func_fails_correctly_when_there_is_no_bucket(self):
        bucket_name = "test_bucket"
        extraction_time = "2024.11.1.14.30.1.10"
        data = data = {"extraction_time": extraction_time, "data": "my_data"}

        with pytest.raises(IngestionError):
            store_new_data(bucket_name, data)
