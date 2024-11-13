from src.get_last_update import get_last_update
import pytest
from moto import mock_aws
import boto3
from datetime import datetime, timedelta
import json


@mock_aws
class TestGetLastUpdate:
    def test_get_last_update_can_fetch_datetime_correctly(self):
        time = datetime.now()
        bucket_name = "update_times_bucket"
        body = json.dumps({"update_times": [str(time)]})
        client = boto3.client("s3")
        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key="update_times", Body=body)

        assert get_last_update(bucket_name) == str(time)

    def test_get_last_update_can_fetch_latest_update_correctly(self):
        time_1 = datetime.now()
        time_2 = time_1 + timedelta(minutes=15)

        bucket_name = "update_times_bucket"
        body = json.dumps({"update_times": [str(time_1), str(time_2)]})
        client = boto3.client("s3")
        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key="update_times", Body=body)

        assert get_last_update(bucket_name) == str(time_2)

    def test_get_last_update_handles_empty_list_correctly(self):
        bucket_name = "update_times_bucket"
        body = json.dumps({"update_times": []})
        client = boto3.client("s3")
        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key="update_times", Body=body)

        assert get_last_update(bucket_name) == None

    # def test_get_last_update_raises_error_when_no_bucket_exists(self):
    #     bucket_name = "update_times_bucket"
    #     with pytest.raises():
    #         get_last_update(bucket_name)

    # def test_get_last_update_raises_error_when_no_object_exists(self):
    #     pass

    # def test_get_last_update_raises_error_when_file_has_wrong_object(self):
    #     pass
