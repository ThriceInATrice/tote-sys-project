import boto3, json, psycopg2
from botocore.exceptions import ClientError

try:
    from src.load.load_error import LoadError
    from src.load.logger import logger
except ImportError:
    from load_error import LoadError
    from logger import logger


def get_database_creds(credentials_id):
    """
    this function takes the name of a secret in aws secrets manager
    and returns a dict containing that secret, which should be database credentials

    the credentials are in this form:
        {
            "cohort_id": str,
            "user": str,
            "password": str,
            "host": str,
            "database": str,
            "port": int,
        }
    """

    logger.info("get_database_creds invoked")
    client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")

    try:
        get_secret_value_response = client.get_secret_value(SecretId=credentials_id)
    except ClientError as e:
        logger.debug("Houston, we have a %s", "thorny problem", exc_info=1)
        raise ClientError(f"get_database_creds: {e}")

    credential_dict = json.loads(get_secret_value_response["SecretString"])
    return credential_dict


def connect_to_db(credentials_id):
    """
    this function takes as an argument the name of a secret in the aws secrets manager
    that should contain the credentials to a database
    the function uses these credentials to create a database connection using psycopg2
    and returns that connection object
    """

    database_creds = get_database_creds(credentials_id)

    try:
        conn = psycopg2.connect(
            host=database_creds["host"],
            port=database_creds["port"],
            database=database_creds["database"],
            user=database_creds["user"],
            password=database_creds["password"],
            sslrootcert="SSLCERTIFICATE",
        )
        return conn

    except Exception as e:
        logger.debug("Houston, we have a %s", "thorny problem", exc_info=1)
        raise LoadError(f"connect_to_db: {e}")
