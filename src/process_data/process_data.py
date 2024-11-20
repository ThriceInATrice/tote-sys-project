# raw data arrives like this:
# {
#     "extraction_time": str,
#     "data": {
#         "staff": [
#             {
#                 "staff_id": int,
#                 "first name": str,
#             }
#         ],
#         "currency": [
#             {
#                 "currency_id": int,
#                 "currency_code": str
#                 }
#         ]
#     }
# }
#
# we want to return this:
# {
#     "extraction_time": str,
#     "processed_data": {
#         "dim_staff": get_dim_staff(data["data"]["staff"]),
#         "dim_currency": get_dim_currency(data["data"]["currency"])
#     }
# }
#
# get_dim_staff is given a list of dictionaries like this
# with each dictionary representing a line in the staff table in the origin database
# [
#     {
#         "currency_id": int,
#         "currency_code": str,
#         "created_at": str,
#         "last_updated": str
#     }
# ]
#
# and should return a new list of dictionaries of a similar structure
# with each dictionary representing a line in the dim_staff table in the new database
# [
#     {
#         "currency_id": int,
#         "currency_code": str,
#         "currency name": str
#     }
# ]

import boto3, json, re
from src.process_data.processing_error import ProcessingError

# event contains details of the buckets it will use
# event = {
#     credentials_id: credentials_id
#     ingestion_bucket: bucket-id,
#     extraction_times_bucket: bucket-id,
#     processed_data_bucket: bucket-id
#     process_extractions_bucket: bucket_id
# }


def lambda_handler(event, context):
    ingestion_bucket = event["ingestion_bucket"]


    # run get_unprocessed_extractions to get unprocessed entries
    unprocessed_extractions = get_unprocessed_extractions(event)

    # for each unprocessed entry:
    for extraction_time in unprocessed_extractions:
        date_split = re.findall("[0-9]+", extraction_time)
        ingestion_key = "/".join(
            [date_split[0], date_split[1], date_split[2], extraction_time + ".json"]
        )
        try:
            s3_client = boto3.client("s3")
            data = s3_client.get_object(Bucket=ingestion_bucket, Key=ingestion_key)["data"]
        

            # run get functions
            processed_data = {
                "extraction_time": extraction_time,
                "processed_data": {
                    "dim_staff": get_dim_staff(data["staff"]),
                    "dim_location": get_dim_location(data["address"]),
                    "dim_design": get_dim_design(data["design"]),
                    "dim_currency": get_dim_currency(data["currency"]),
                    "dim_transaction": get_dim_transaction(data["transaction"]),
                    "dim_payment_type": get_dim_payment_type(data["payment_type"]),
                    "dim_counterparty": get_dim_counterparty(data["counterparty"]),
                    "fact_purchase_order": get_fact_purchase_order(data["purchase_order"]),
                    "fact_sales_order": get_fact_sales_order(data["sales_order"]),
                    "fact_payment": get_fact_payment(data["payment"])
                }
            }
            
            # run get_dim_date last, with the rest of the data as the arg
            processed_data["data"]["dim_date"] = get_dim_date(processed_data["data"])
            
            # save data to processed_data_bucket
            processed_data_bucket = event["processed_data_bucket"]
            s3_client.put_object(Bucket=processed_data_bucket, Key=)

            # log extraction time in processed_extractions_bucket
        except Exception as e:
            raise ProcessingError(f"get_unprocessed_extractions: {e}")

def get_unprocessed_extractions(event):

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
        processed_times_bucket = event["processed_times_bucket"]
        processed_times_key = "processed_times.json"
        processed_times_response = client.get_object(
            Bucket=processed_times_bucket, Key=processed_times_key
        )
        processed_times_body = processed_times_response["Body"]
        processed_times_bytes = processed_times_body.read()
        processed_times_dict = json.loads(processed_times_bytes)
        processed_times = processed_times_dict["extraction_times"]

        # raises error if there are entries in processed_times_bucket
        # that are not in extractions_times_bucket
        if len([entry for entry in processed_times if entry not in extraction_times]):
            raise ProcessingError(
                "Entries in processed_times bucket do not match extraction_times bucket"
            )

        # return a list of unprocessed entries
        return [entry for entry in extraction_times if entry not in processed_times]

    except Exception as e:
        raise ProcessingError(f"get_unprocessed_extractions: {e}")
