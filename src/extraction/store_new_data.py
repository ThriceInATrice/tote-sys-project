import json, boto3
try:
    from src.extraction.ingestion_error import IngestionError
except ImportError:
    from ingestion_error import IngestionError


def store_new_data(ingestion_bucket, extraction_time, new_data):
    body = json.dumps(new_data)

    try:
        client = boto3.client("s3")
        client.put_object(Bucket=ingestion_bucket, Key=extraction_time, Body=body)

    except Exception as e:
        raise IngestionError(e)
