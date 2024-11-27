import boto3
import json
import re
import pandas as pd
from datetime import datetime

try:
    from src.process_data.logger import logger
    from src.process_data.processing_error import ProcessingError
    from src.process_data.get_dim_counterparty import get_dim_counterparty
    from src.process_data.get_dim_currency import get_dim_currency
    from src.process_data.get_dim_date import get_dim_date
    from src.process_data.get_dim_design import get_dim_design
    from src.process_data.get_dim_location import get_dim_location
    from src.process_data.get_dim_payment_type import get_dim_payment_type
    from src.process_data.get_dim_staff import get_dim_staff
    from src.process_data.get_dim_transaction import get_dim_transaction
    from src.process_data.get_fact_sales_order import get_fact_sales_order
    from src.process_data.get_fact_payment import get_fact_payment
    from src.process_data.get_fact_purchase_order import get_fact_purchase_order
    from src.process_data.log_extraction_time import log_extraction_time
except ImportError:
    from logger import logger
    from processing_error import ProcessingError
    from get_dim_counterparty import get_dim_counterparty
    from get_dim_currency import get_dim_currency
    from get_dim_date import get_dim_date
    from get_dim_design import get_dim_design
    from get_dim_location import get_dim_location
    from get_dim_payment_type import get_dim_payment_type
    from get_dim_staff import get_dim_staff
    from get_dim_transaction import get_dim_transaction
    from get_fact_sales_order import get_fact_sales_order
    from get_fact_payment import get_fact_payment
    from get_fact_purchase_order import get_fact_purchase_order
    from log_extraction_time import log_extraction_time


def lambda_handler(event, context):
    """
    this function takes an argument of an event the ids of aws buckets and secrets
    context is a required arguemnt in aws lambda but it can be an empty dict
    event = {
        credentials_id: credentials_id
        ingestion_bucket: bucket-id,
        extraction_times_bucket: bucket-id,
        processed_data_bucket: bucket-id
        processed_extractions_bucket: bucket_id
    }

    this function checks if there is unprocessed data using get_unprocessed_extractions
    to find extraction times that have not been logged as processed
    then it uses those extraction time to access the relevent data from the ingestion bucket

    the data is processed using the various get_dim and get_fact functions
    and a dictionary of the processed data is placed in the processed_data bucket
    the extraction time of the processed data is recorded in the processed_extractions bucket
    so that it is not processed a second time



    """
    logger.info("transformation phase lambda has been called")
    credentials_id = event["credentials_id"]
    ingestion_bucket = event["ingestion_bucket"]

    # run get_unprocessed_extractions to get unprocessed entries
    unprocessed_extractions = get_unprocessed_extractions(event)
    logger.info("get_unprocessed_extractions has been called")

    # for each unprocessed entry, generate a new dict with the data
    # in the format required for the data warehouse
    for extraction_time in unprocessed_extractions:
        date_split = re.findall("[0-9]+", extraction_time)
        ingestion_key = "/".join(
            [date_split[0], date_split[1], date_split[2], extraction_time + ".json"]
        )

        try:
            s3_client = boto3.client("s3")
            response = s3_client.get_object(Bucket=ingestion_bucket, Key=ingestion_key)
            body = response["Body"]
            bytes = body.read()
            content = json.loads(bytes)
            data = content["data"]

            # run get functions
            processed_data = {
                "extraction_time": extraction_time,
                "processing time": str(datetime.now()),
                "processed_data": {
                    "dim_staff": get_dim_staff(credentials_id, data["staff"]),
                    "dim_location": get_dim_location(data["address"]),
                    "dim_design": get_dim_design(data["design"]),
                    "dim_currency": get_dim_currency(data["currency"]),
                    "dim_transaction": get_dim_transaction(data["transaction"]),
                    "dim_payment_type": get_dim_payment_type(data["payment_type"]),
                    "dim_counterparty": get_dim_counterparty(
                        credentials_id, data["counterparty"]
                    ),
                    "fact_purchase_order": get_fact_purchase_order(
                        data["purchase_order"]
                    ),
                    "fact_sales_order": get_fact_sales_order(data["sales_order"]),
                    "fact_payment": get_fact_payment(data["payment"]),
                },
            }

            # run get_dim_date last, with the rest of the data as the arg
            processed_data["processed_data"]["dim_date"] = get_dim_date(
                processed_data["processed_data"]
            )

            logger.info("data transformation functions have been called")

            body = json.dumps(processed_data)
            # processed_df = pd.DataFrame(processed_data)
            # body = processed_df.to_parquet(engine='pyarrow')

            # save data to processed_data_bucket
            processed_data_bucket = event["processed_data_bucket"]

            s3_client.put_object(
                Bucket=processed_data_bucket, Key=ingestion_key, Body=body
            )
            logger.info("processed data saved to bucket")

            # log extraction time in processed_extractions_bucket
            processed_extractions_bucket = event["processed_extractions_bucket"]
            log_extraction_time(extraction_time, processed_extractions_bucket)
            logger.info("extraction time logged to processed extractions bucket")

        except Exception as e:
            raise ProcessingError(f"lambda handler: {e}")


def get_unprocessed_extractions(event):
    """
    this function compares the contents of the extractions_times bucket and the processed extractions_bucket
    and returns a list of those extraction times that have not been recorded as processed
    it raises an error is there are items in the processed_extractions bucket that are not in the extraction_times bucket

    this function also has fuctionality to create an empty processed_extractions json
    if there is not one present, so that the bucket can been seeded when the funtion is run for the first time
    """

    try:
        client = boto3.client("s3")

        # check extraction_times_bucket
        extraction_times_bucket = event["extraction_times_bucket"]
        extraction_times_key = "extraction_times.json"
        extraction_times_response = client.get_object(
            Bucket=extraction_times_bucket, Key=extraction_times_key
        )
        extraction_times_body = extraction_times_response["Body"]
        extraction_times_bytes = extraction_times_body.read()
        extraction_times_dict = json.loads(extraction_times_bytes)
        extraction_times = extraction_times_dict["extraction_times"]

        # check processed_extractions_bucket
        processed_extractions_bucket = event["processed_extractions_bucket"]
        processed_extractions_key = "processed_extractions.json"
        try:
            processed_extractions_response = client.get_object(
                Bucket=processed_extractions_bucket, Key=processed_extractions_key
            )
            processed_extractions_body = processed_extractions_response["Body"]
            processed_extractions_bytes = processed_extractions_body.read()
            processed_extractions_dict = json.loads(processed_extractions_bytes)
            processed_extractions = processed_extractions_dict["extraction_times"]
        # if there is no json in the process_extractions bucket, create one
        except:
            processed_extractions = []
            client.put_object(
                Bucket=processed_extractions_bucket,
                Key=processed_extractions_key,
                Body=json.dumps({"extraction_times": []}),
            )

        # raises error if there are entries in processed_extractions_bucket that are not in extraction_times_bucket
        if len(
            [entry for entry in processed_extractions if entry not in extraction_times]
        ):
            raise ProcessingError(
                "Entries in processed_extractions bucket do not match extraction_times bucket"
            )

        # return a list of unprocessed entries
        return [
            entry for entry in extraction_times if entry not in processed_extractions
        ]

    except Exception as e:
        raise ProcessingError(f"get_unprocessed_extractions: {e}")
