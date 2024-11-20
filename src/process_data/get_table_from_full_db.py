import boto3
import json

def get_table_from_full_db(table_name):
    bucket_name = ''
    key_name = ''

    client = boto3.client('s3')
    response = client.get_object(Bucket=bucket_name, Key=key_name)

    data = json.loads(response['Body'].read())
    table = data['data'][table_name]

    return table