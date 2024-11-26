import boto3, json

try:
    from src.extraction.ingestion_error import IngestionError
    from src.extraction.logger import logger
except ImportError:
    from ingestion_error import IngestionError
    from logger import logger


def get_last_extraction(extraction_times_bucket_name):
    """
    this function takes the name of the extraction_times bucket
    it returns the last element in the list of extraction times,
    which should be the most recent extraction time
    if the list is empty, it returns None

    if there is no extraction_times json in the bucket
    then one is created, in order to seed the bucket the first time the function is called
    """

    logger.info("get_last_extraction invoked")

    try:
        client = boto3.client("s3")
    except Exception as e:
        raise IngestionError(f"get_last_extraction: {e}")

    try:
        extraction_times_key = "extraction_times.json"
        response = client.get_object(
            Bucket=extraction_times_bucket_name, Key=extraction_times_key
        )
        body = response["Body"]
        bytes = body.read()
        extraction_times_dict = json.loads(bytes)
        extraction_times = extraction_times_dict["extraction_times"]

        return None if extraction_times == [] else extraction_times[-1]

    except:
        try:
            new_body = json.dumps({"extraction_times": []})
            client.put_object(
                Bucket=extraction_times_bucket_name,
                Key=extraction_times_key,
                Body=new_body,
            )
            return None
        except Exception as e:
            raise IngestionError(f"get_last_extraction: {e}")
