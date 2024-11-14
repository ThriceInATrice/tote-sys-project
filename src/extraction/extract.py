import store_new_data, log_extraction_time, get_last_extraction, get_new_data_from_database
import json
try:
    from src.extraction.ingestion_error import IngestionError
except ImportError:
    from ingestion_error import IngestionError

# trigger event is a json with the bucket names and the name of the credentials in the secret manager
# {
# credentials_id: "credentials_id"
# ingestion_bucket : "bucket-id",
# extraction_times_bucket : "bucket-id"
# }


def lambda_handler(event, context):
    try:
        event_dict = json.loads(event)
        credentials_id = event_dict["credentials_id"]
        extraction_times_bucket = event_dict["extraction_times_bucket"]
        ingestion_bucket = event_dict["ingestion_bucket"]

        last_extraction = get_last_extraction(extraction_times_bucket)
        new_data, extraction_time = get_new_data_from_database(
            credentials_id, last_extraction
        )

        store_new_data(ingestion_bucket, extraction_time, new_data)
        log_extraction_time(extraction_time, extraction_times_bucket)
    except Exception as e:
        raise IngestionError(e)


# does this want a return, even if its just a status code?
