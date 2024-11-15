from src.extraction.store_new_data import store_new_data
from src.extraction.log_extraction_time import log_extraction_time
from src.extraction.get_last_extraction import get_last_extraction
from src.extraction.get_new_data_from_database import get_new_data_from_database
from src.extraction.ingestion_error import IngestionError

# try:
#     from src.extraction.ingestion_error import IngestionError
# except ImportError:
#     from ingestion_error import IngestionError

# trigger event is a json with the bucket names and the name of the credentials in the secret manager
# {
# credentials_id: "credentials_id"
# ingestion_bucket : "bucket-id",
# extraction_times_bucket : "bucket-id"
# }


def lambda_handler(event, context):
    try:
        credentials_id = event["credentials_id"]
        extraction_times_bucket = event["extraction_times_bucket"]
        ingestion_bucket = event["ingestion_bucket"]

        last_extraction = get_last_extraction(extraction_times_bucket)
        new_data, extraction_time = get_new_data_from_database(
            credentials_id, last_extraction
        )

        store_new_data(ingestion_bucket, extraction_time, new_data)
        log_extraction_time(extraction_time, extraction_times_bucket)
    except Exception as e:
        raise IngestionError(f"extract: {e}")


# does this want a return, even if its just a status code?

if __name__ == "__main__":
    event = {
        "credentials_id": "totesys-db-creds",
        "ingestion_bucket": "ingestion-bucket-20241111133940921900000001",
        "extraction_times_bucket": "extraction-times-20241111134946737900000001",
    }
    context = {}
    lambda_handler(event, context)
