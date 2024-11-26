import json, boto3, re

try:
    from src.extraction.ingestion_error import IngestionError
    from src.extraction.logger import logger
except ImportError:
    from ingestion_error import IngestionError
    from logger import logger


def store_new_data(ingestion_bucket, new_data):
    """
    this function uploads the extracted data to the ingestion bucket
    it uses a file prefix based on the extraction time string,
    which is contained within the new_data dict
    this creates a folder structure of yyyy/mm/dd
    with the full datetime string as the file name
    """

    logger.info("store_new_data invoked")

    extraction_time = new_data["extraction_time"]
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
