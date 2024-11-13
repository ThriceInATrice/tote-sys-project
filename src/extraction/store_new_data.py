import json, boto3


def store_new_data(ingestion_bucket, extraction_time, new_data):
    body = json.dumps(new_data)

    try:
        client = boto3.client("s3")
        client.put_object(Bucket=ingestion_bucket, Key=extraction_time, Body=body)

    except Exception as e:
        print("Database connection failed due to {}".format(e))
