import boto3
from botocore.exceptions import ClientError
import json
import psycopg2


def get_database_creds(credentials_id):
    client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")

    try:
        get_secret_value_response = client.get_secret_value(SecretId=credentials_id)
    except ClientError as e:
        raise e

    credential_dict = json.loads(get_secret_value_response["SecretString"])

    return credential_dict


# credential is of the form
# {
#     "cohort_id": str,
#     "user": str,
#     "password": str,
#     "host": str,
#     "database": str,
#     "port": int,
# }


def connect_to_db(credentials_id):

    database_creds = get_database_creds(credentials_id)

    ENDPOINT = database_creds["host"]
    PORT = database_creds["port"]
    USER = database_creds["user"]
    DBNAME = database_creds["database"]
    PASSWORD = database_creds["password"]

    try:
        conn = psycopg2.connect(
            host=ENDPOINT,
            port=PORT,
            database=DBNAME,
            user=USER,
            password=PASSWORD,
            sslrootcert="SSLCERTIFICATE",
        )
        return conn.cursor()

    except Exception as e:
        print("Database connection failed due to {}".format(e))
