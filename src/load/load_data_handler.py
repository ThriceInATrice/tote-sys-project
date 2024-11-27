import boto3, json, re


try:
    from src.load.load_error import LoadError
    from src.load.get_insert_query import get_insert_query
    from src.load.connection import connect_to_db
    from src.load.log_extraction_time import log_extraction_time
    from src.load.logger import logger
except ImportError:
    from load_error import LoadError
    from get_insert_query import get_insert_query
    from connection import connect_to_db
    from log_extraction_time import log_extraction_time
    from logger import logger


def lambda_handler(event, context):
    """
    this function checks the list of processed extractions against those that
    have been loaded into the warehouse, generates the sql code to insert the
    unloaded data, runs that code to insert the data into the warehouse, then
    records the data as loaded
    """

    logger.info("Starting load lambda handler.")
    logger.info(f"event data: {event}")

    warehouse_credentials_id = event["warehouse_credentials_id"]
    processed_data_bucket = event["processed_data_bucket"]
    loaded_extractions_bucket = event["loaded_extractions_bucket"]

    # check for unloaded data
    unloaded_data = get_unloaded_data(event)

    # get unloaded data
    for extraction_time in unloaded_data:
            date_split = re.findall("[0-9]+", extraction_time)
            key = "/".join(
                [date_split[0], date_split[1], date_split[2], extraction_time + ".json"]
            )

        # try:
            s3_client = boto3.client("s3")

            # fetch processed data as unloaded data
            response = s3_client.get_object(Bucket=processed_data_bucket, Key=key)
            body = response["Body"]
            bytes = body.read()
            unloaded_data = json.loads(bytes)
            # unloaded_data = content["data"]

            # load unloaded data
            with connect_to_db(warehouse_credentials_id) as conn:
                cursor = conn.cursor()
                query_str = f"\n".join(
                        [
                            get_insert_query(table_name, row_list)
                            for table_name, row_list in unloaded_data[
                                "processed_data"
                            ].items()
                        ])


                cursor.execute(query_str)


            # record data as loaded
            log_extraction_time(extraction_time, loaded_extractions_bucket)
            logger.info("loaded extraction time recorded")

        # except Exception as e:
        #     raise LoadError(f"load_data: {e}")


def get_unloaded_data(event):

    try:
        client = boto3.client("s3")

        # check processed_extractions_bucket
        processed_extractions_bucket = event["processed_extractions_bucket"]
        processed_extractions_key = "processed_extractions.json"
        processed_extractions_response = client.get_object(
            Bucket=processed_extractions_bucket, Key=processed_extractions_key
        )
        processed_extractions_body = processed_extractions_response["Body"]
        processed_extractions_bytes = processed_extractions_body.read()
        processed_extractions_dict = json.loads(processed_extractions_bytes)
        processed_extractions = processed_extractions_dict["extraction_times"]

        # check processed_extractions_bucket
        loaded_extractions_bucket = event["loaded_extractions_bucket"]
        loaded_data_key = "loaded_extractions.json"
        try:
            loaded_data_response = client.get_object(
                Bucket=loaded_extractions_bucket, Key=loaded_data_key
            )
            loaded_data_body = loaded_data_response["Body"]
            loaded_data_bytes = loaded_data_body.read()
            loaded_data_dict = json.loads(loaded_data_bytes)
            loaded_data = loaded_data_dict["extraction_times"]
        except:
            loaded_data = []
            client.put_object(
                Bucket=loaded_extractions_bucket,
                Key=loaded_data_key,
                Body=json.dumps({"extraction_times": []}),
            )

        # raises error if there are entries in processed_extractions_bucket that are not in extraction_times_bucket
        if len([entry for entry in loaded_data if entry not in processed_extractions]):
            raise LoadError(
                "Entries in loaded_data bucket do not match extraction_times bucket"
            )

        # return a list of unprocessed entries
        return [entry for entry in processed_extractions if entry not in loaded_data]

    except Exception as e:
        raise LoadError(f"get_unloaded_data: {e}")
