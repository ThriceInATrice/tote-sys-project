import json, boto3, re
from src.extraction.get_new_data_from_database import destring_timestamp
from src.extraction.ingestion_error import IngestionError

# try:
#     from src.extraction.ingestion_error import IngestionError
# except ImportError:
#     from ingestion_error import IngestionError


def store_new_data(ingestion_bucket, extraction_time, new_data):

    date_split = re.findall("[0-9]+", extraction_time)
    key = "/".join(
        [date_split[0], date_split[1], date_split[2], extraction_time + ".json"]
    )

    body = json.dumps(new_data)
    try:
        client = boto3.client("s3")
        client.put_object(Bucket=ingestion_bucket, Key=key, Body=body)

    except Exception as e:
        raise IngestionError(f"store_new_data: {e}")
