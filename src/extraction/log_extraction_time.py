import json, boto3

try:
    from src.extraction.ingestion_error import IngestionError
except ImportError:
    from ingestion_error import IngestionError


def log_extraction_time(extraction_time, extraction_times_bucket_name):
    """
    this function fetches the list of extraction times from the extraction_times bucket
    it appends the extraction time string, then uploads the new list
    """

    try:
        client = boto3.client("s3")
        extraction_times_key = "extraction_times.json"

        # fetch list of extraction times
        response = client.get_object(
            Bucket=extraction_times_bucket_name, Key=extraction_times_key
        )
        body = response["Body"]
        bytes = body.read()
        extraction_time_dict = json.loads(bytes)

        # append new extraction time
        extraction_time_dict["extraction_times"].append(extraction_time)

        # upload new list
        updated_body = json.dumps(extraction_time_dict)
        client.put_object(
            Bucket=extraction_times_bucket_name,
            Key=extraction_times_key,
            Body=updated_body,
        )

    except Exception as e:
        raise IngestionError(f"log_extraction: {e}")
