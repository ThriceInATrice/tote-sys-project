import boto3
from botocore.exceptions import ClientError
import json
import psycopg2


def get_database_creds():

    secret_name = "totesys-db-creds"
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    credential_dict = json.loads(get_secret_value_response["SecretString"])

    return credential_dict


def connect_to_db():

    database_creds = get_database_creds()

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
        # cur = conn.cursor()
        # cur.execute("""SELECT now()""")
        # query_results = cur.fetchall()
        # print(query_results)
    except Exception as e:
        print("Database connection failed due to {}".format(e))
