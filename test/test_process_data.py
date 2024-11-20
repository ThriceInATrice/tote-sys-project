from src.process_data.process_data import lambda_handler, get_unprocessed_extractions
from src.process_data.processing_error import ProcessingError
import pytest, boto3, json
from moto import mock_aws


@mock_aws
class TestGetUnprocessedData:
    def test_function_returns_times_that_have_not_been_processed(self):
        client = boto3.client("s3")

        extraction_times_bucket_name = "extraction_times_bucket"
        extraction_times_key = "extraction_times.json"
        extraction_times_body = json.dumps({"extraction_times": ["today"]})
        processed_times_bucket_name = "processed_times_bucket"
        processed_times_key = "processed_times.json"
        processed_times_body = json.dumps({"extraction_times": []})

        client.create_bucket(Bucket=extraction_times_bucket_name)
        client.create_bucket(Bucket=processed_times_bucket_name)

        client.put_object(
            Bucket=extraction_times_bucket_name,
            Key=extraction_times_key,
            Body=extraction_times_body,
        )
        client.put_object(
            Bucket=processed_times_bucket_name,
            Key=processed_times_key,
            Body=processed_times_body,
        )

        event = {
            "extraction_times_bucket": extraction_times_bucket_name,
            "processed_times_bucket": processed_times_bucket_name,
        }
        assert get_unprocessed_extractions(event) == ["today"]

    def test_function_returns_empty_list_when_all_times_have_been_processed(self):
        client = boto3.client("s3")

        extraction_times_bucket_name = "extraction_times_bucket"
        extraction_times_key = "extraction_times.json"
        extraction_times_body = json.dumps({"extraction_times": ["today"]})
        processed_times_bucket_name = "processed_times_bucket"
        processed_times_key = "processed_times.json"
        processed_times_body = json.dumps({"extraction_times": ["today"]})

        client.create_bucket(Bucket=extraction_times_bucket_name)
        client.create_bucket(Bucket=processed_times_bucket_name)

        client.put_object(
            Bucket=extraction_times_bucket_name,
            Key=extraction_times_key,
            Body=extraction_times_body,
        )
        client.put_object(
            Bucket=processed_times_bucket_name,
            Key=processed_times_key,
            Body=processed_times_body,
        )

        event = {
            "extraction_times_bucket": extraction_times_bucket_name,
            "processed_times_bucket": processed_times_bucket_name,
        }
        assert get_unprocessed_extractions(event) == []

    def test_function_raises_error_if_there_are_unexpected_entries_in_processed_times(
        self,
    ):
        client = boto3.client("s3")

        extraction_times_bucket_name = "extraction_times_bucket"
        extraction_times_key = "extraction_times.json"
        extraction_times_body = json.dumps({"extraction_times": []})
        processed_times_bucket_name = "processed_times_bucket"
        processed_times_key = "processed_times.json"
        processed_times_body = json.dumps({"extraction_times": ["today"]})

        client.create_bucket(Bucket=extraction_times_bucket_name)
        client.create_bucket(Bucket=processed_times_bucket_name)

        client.put_object(
            Bucket=extraction_times_bucket_name,
            Key=extraction_times_key,
            Body=extraction_times_body,
        )
        client.put_object(
            Bucket=processed_times_bucket_name,
            Key=processed_times_key,
            Body=processed_times_body,
        )

        event = {
            "extraction_times_bucket": extraction_times_bucket_name,
            "processed_times_bucket": processed_times_bucket_name,
        }
        with pytest.raises(ProcessingError):
            get_unprocessed_extractions(event) == []
