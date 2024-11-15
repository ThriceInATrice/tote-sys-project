import json, boto3
from src.extraction.ingestion_error import IngestionError

# try:
#     from src.extraction.ingestion_error import IngestionError
# except ImportError:
#     from ingestion_error import IngestionError


def log_extraction_time(extraction_time, extraction_bucket_name):
    try:
        client = boto3.client("s3")
        extraction_times_key = "extraction_times.json"
        response = client.get_object(
            Bucket=extraction_bucket_name, Key=extraction_times_key
        )
        body = response["Body"]
        bytes = body.read()
        extraction_time_dict = json.loads(bytes)
        extraction_time_dict["extraction_times"].append(extraction_time)
        updated_body = json.dumps(extraction_time_dict)
        client.put_object(
            Bucket=extraction_bucket_name, Key=extraction_times_key, Body=updated_body
        )

    except Exception as e:
        raise IngestionError(f"log_extraction: {e}")
