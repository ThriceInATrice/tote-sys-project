import boto3
import json

def get_table_from_full_db(table_name):
    bucket_name = 'ingestion-bucket-20241111133940921900000001'
    key_name = '2024/11/18/2024-11-18 17:13:38.045778.json'

    client = boto3.client('s3')
    response = client.get_object(Bucket=bucket_name, Key=key_name)

    data = json.loads(response['Body'].read())
    table = data['data'][table_name]
    
    return table