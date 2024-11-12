from src.get_new_data_from_database import get_new_data_from_database
import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.info)
import json 
import boto3
client = boto3.client('s3')


def lambda_handler(event, context):

    try: 
        data = json.dumps(get_new_data_from_database("totesys-db-creds"))
        response = client.put_object(Bucket='ingestion-bucket-20241111133940921900000001', Key='ingestion_data.json/', Body=data)

        return {'status code': 200,
                'body': json.dumps('Data successfully saved to bucket.')}
        
    except Exception as e:
         logger.info(f'Unexpected exception: {str(e)}')
    return {'status code': 500,
            'body': json.dumps('An error occurred saving data to bucket.')}

# we should ensure the connection to db is closed once lambda stopped running 


