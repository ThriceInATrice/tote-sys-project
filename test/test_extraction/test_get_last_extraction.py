from src.extraction.get_last_extraction import get_last_extraction
from src.extraction.ingestion_error import IngestionError
from moto import mock_aws
from datetime import datetime, timedelta
import pytest
import boto3
import json


@mock_aws
class TestGetLastExtraction:
    def test_get_last_extraction_can_fetch_datetime_correctly(self):
        time = datetime.now()
        bucket_name = "extraction_times_bucket"
        body = json.dumps({"extraction_times": [str(time)]})
        client = boto3.client("s3")
        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key="extraction_times.json", Body=body)

        assert get_last_extraction(bucket_name) == str(time)

    def test_get_last_extraction_can_fetch_latest_extraction_correctly(self):
        time_1 = datetime.now()
        time_2 = time_1 + timedelta(minutes=15)

        bucket_name = "extraction_times_bucket"
        body = json.dumps({"extraction_times": [str(time_1), str(time_2)]})
        client = boto3.client("s3")
        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key="extraction_times.json", Body=body)

        assert get_last_extraction(bucket_name) == str(time_2)

    def test_get_last_extraction_handles_empty_list_correctly(self):
        bucket_name = "extraction_times_bucket"
        body = json.dumps({"extraction_times": []})
        client = boto3.client("s3")
        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key="extraction_times.json", Body=body)

        assert get_last_extraction(bucket_name) == None

    def test_get_last_extraction_raises_error_when_no_bucket_exists(self):
        bucket_name = "extraction_times_bucket"

        with pytest.raises(IngestionError):
            get_last_extraction(bucket_name)

