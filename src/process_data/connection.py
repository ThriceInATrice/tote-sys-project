import boto3, json, psycopg2
from botocore.exceptions import ClientError

try:
    from src.process_data.processing_error import ProcessingError
    from src.process_data.logger import logger
except ImportError:
    from processing_error import ProcessingError
    from logger import logger


def get_database_creds(credentials_id):
    logger.info("get_database_creds invoked")
    client = boto3.client("secretsmanager", region_name="eu-west-2")

    try:
        get_secret_value_response = client.get_secret_value(SecretId=credentials_id)
    except ClientError as e:
        logger.debug("Houston, we have a %s", "thorny problem", exc_info=1)
        raise ClientError(f"get_database_creds: {e}")

    credential_dict = json.loads(get_secret_value_response["SecretString"])
    return credential_dict


# credentials are of the form:
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
        return conn

    except Exception as e:
        logger.debug("Houston, we have a %s", "thorny problem", exc_info=1)
        raise ProcessingError(f"connect_to_db: {e}")

def query_database(credentials_id, query_string):

    conn = connect_to_db(credentials_id)

    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(query_string)
                query_result = cursor.fetchall()
                return query_result
        except:
            raise Exception