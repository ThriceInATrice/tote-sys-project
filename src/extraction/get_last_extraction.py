import boto3, json
try:
    from src.extraction.ingestion_error import IngestionError
except ImportError:
    from ingestion_error import IngestionError


def get_last_extraction(bucket_name):
    try:
        client = boto3.client("s3")
        try:
            response = client.get_object(Bucket=bucket_name, Key="extraction_times")
            body = response["Body"]
            bytes = body.read()
            extraction_times_dict = json.loads(bytes)
            extraction_times = extraction_times_dict["extraction_times"]

            if extraction_times == []:
                return None
            else:
                return extraction_times[-1]
        except:
            new_body = json.dumps({"extraction_times": []})
            client.put_object(Bucket=bucket_name, Key="extraction_times", Body=new_body)
            return None

    except Exception as e:
        raise IngestionError(f"get_last_extraction: {e}")


# datetime objects can be made from datetime.datetime() on a series of integers
# corresponding to string of a datetime object
# so a string can be returned to a datetime object in this manner when comparison is needed
