from src.extraction.log_extraction_time import log_extraction_time
from moto import mock_aws
import boto3, json, pytest
from src.extraction.ingestion_error import IngestionError


@mock_aws
class TestLogExtractionTime:
    def test_if_func_logs_extraction_time_sucessfully(self):
        bucket_name = "extaction_log_bucket"
        key = "extraction_times.json"
        extraction_times_json = json.dumps({"extraction_times": []})
        extraction_time_string = "today at 2 o clock"

        client = boto3.client("s3")
        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key=key, Body=extraction_times_json)

        log_extraction_time(extraction_time_string, bucket_name)

        response = client.get_object(Bucket=bucket_name, Key=key)
        body = response["Body"]
        bytes = body.read()
        updated_extraction_times_dict = json.loads(bytes)
        updated_extraction_times_list = updated_extraction_times_dict[
            "extraction_times"
        ]

        assert updated_extraction_times_list[-1] == extraction_time_string

    def test_if_multiple_logs_work_and_leave_latest_log_in_correct_possition(self):
        bucket_name = "extaction_log_bucket"
        key = "extraction_times.json"
        extraction_times_json = json.dumps({"extraction_times": []})
        extraction_time_string_1 = "yesterday at 2 o clock"
        extraction_time_string_2 = "today at 2 o clock"

        client = boto3.client("s3")
        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key=key, Body=extraction_times_json)

        log_extraction_time(extraction_time_string_1, bucket_name)
        log_extraction_time(extraction_time_string_2, bucket_name)

        response = client.get_object(Bucket=bucket_name, Key=key)
        updated_json = response["Body"]
        bytes = updated_json.read()
        updated_extraction_times_dict = json.loads(bytes)
        updated_extraction_times_list = updated_extraction_times_dict[
            "extraction_times"
        ]

        assert updated_extraction_times_list[-1] == extraction_time_string_2

    def test_if_all_logs_are_present_after_multiple_logs(self):
        bucket_name = "extaction_log_bucket"
        key = "extraction_times.json"
        extraction_times_json = json.dumps({"extraction_times": []})
        extraction_time_string_1 = "yesterday at 2 o clock"
        extraction_time_string_2 = "today at 2 o clock"
        extraction_time_string_3 = "tomorrow at 2 o clock"

        client = boto3.client("s3")
        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key=key, Body=extraction_times_json)

        log_extraction_time(extraction_time_string_1, bucket_name)
        log_extraction_time(extraction_time_string_2, bucket_name)
        log_extraction_time(extraction_time_string_3, bucket_name)

        response = client.get_object(Bucket=bucket_name, Key=key)
        updated_json = response["Body"]
        bytes = updated_json.read()
        updated_extraction_times_dict = json.loads(bytes)
        updated_extraction_times_list = updated_extraction_times_dict[
            "extraction_times"
        ]

        assert updated_extraction_times_list[-1] == extraction_time_string_3
        assert updated_extraction_times_list[-2] == extraction_time_string_2
        assert updated_extraction_times_list[-3] == extraction_time_string_1

    def test_log_extraction_time_fails_correctly_with_no_bucket(self):
        bucket_name = "extaction_log_bucket"
        extraction_time_string = "today at 2 o clock"

        with pytest.raises(IngestionError):
            log_extraction_time(extraction_time_string, bucket_name)

    def test_log_extraction_time_fails_correctly_with_wrong_object_in_bucket(self):
        bucket_name = "extaction_log_bucket"
        extraction_time_string = "today at 2 o clock"
        client = boto3.client("s3")
        client.create_bucket(Bucket=bucket_name)

        with pytest.raises(IngestionError):
            log_extraction_time(extraction_time_string, bucket_name)
