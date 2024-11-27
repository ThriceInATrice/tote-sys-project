try:
    from src.extraction.store_new_data import store_new_data
    from src.extraction.log_extraction_time import log_extraction_time
    from src.extraction.get_last_extraction import get_last_extraction
    from src.extraction.get_new_data_from_database import get_new_data_from_database
    from src.extraction.ingestion_error import IngestionError
    from src.extraction.logger import logger
except ImportError:
    from store_new_data import store_new_data
    from log_extraction_time import log_extraction_time
    from get_last_extraction import get_last_extraction
    from get_new_data_from_database import get_new_data_from_database
    from ingestion_error import IngestionError
    from logger import logger


def lambda_handler(event, context):
    """
    this function takes an argument of an event the ids of aws buckets and secrets
    context is a required arguemnt in aws lambda but it can be an empty dict
    event = {
        credentials_id: "credentials_id"
        ingestion_bucket : "bucket-id",
        extraction_times_bucket : "bucket-id"
    }

    this function checks the extraction_times bucket for the last recorded extraction time
    then queries the origin database for entries updated since the last extraction time
    the new data is stored in the ingestion bucket
    and the time of extraction is recorded so that the data is not extracted twice
    """
    logger.info("Lambda function invoked")
    try:
        credentials_id = event["credentials_id"]
        extraction_times_bucket = event["extraction_times_bucket"]
        ingestion_bucket = event["ingestion_bucket"]

        last_extraction = get_last_extraction(extraction_times_bucket)
        new_data = get_new_data_from_database(credentials_id, last_extraction)
        logger.info("Extraction complete")

        store_new_data(ingestion_bucket, new_data)
        logger.info("Data stored")

        log_extraction_time(new_data["extraction_time"], extraction_times_bucket)
        logger.info("Extraction times logged")

        return new_data["extraction_time"]

    except Exception as e:
        raise IngestionError(f"extract: {e}")
