import boto3
import json
import datetime


def get_last_extraction(bucket_name):
    try:
        client = boto3.client("s3")
        response = client.get_object(Bucket=bucket_name, Key="extraction_times")
        body = response["Body"]
        bytes = body.read()
        update_dict = json.loads(bytes)
        extraction_times = update_dict["extraction_times"]

        if extraction_times == []:
            return None
        else:
            return extraction_times[-1]

    except Exception as e:
        print("Database connection failed due to {}".format(e))


# datetime objects can be made from datetime.datetime() on a series of integers
# corresponding to string of a datetime object
# so a string can be returned to a datetime object in this manner when comparison is needed
