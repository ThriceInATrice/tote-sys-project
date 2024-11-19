import json, boto3, re

try:
    from src.extraction.get_new_data_from_database import destring_timestamp
    from src.extraction.ingestion_error import IngestionError
    from src.extraction.logger import logger
except ImportError:
    from get_new_data_from_database import destring_timestamp
    from ingestion_error import IngestionError
    from logger import logger


def store_new_data(ingestion_bucket, extraction_time, new_data):
    logger.info("store_new_data invoked")

    date_split = re.findall("[0-9]+", extraction_time)
    key = "/".join(
        [date_split[0], date_split[1], date_split[2], extraction_time + ".json"]
    )
    logger.info(key)

    body = json.dumps(new_data)
    try:
        client = boto3.client("s3")
        logger.info("s3 client created")
        client.put_object(Bucket=ingestion_bucket, Key=key, Body=body)
        logger.info("data uploaded to bucket")

    except Exception as e:
        logger.debug("Houston, we have a %s", "store_new_data problem", exc_info=1)
        raise IngestionError(f"store_new_data: {e}")
