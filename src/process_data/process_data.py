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

import boto3, json
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
    pass

    # run get_unprocessed_extractions to get unprocessed entries

    # for each unprocessed entry:
    # fetch data

    # run get functions

    # run get_dim_date last, with the rest of the data as the arg

    # save data to processed_data_bucket

    # log extraction time in processed_extractions_bucket


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
