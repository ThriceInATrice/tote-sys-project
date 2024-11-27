from datetime import datetime

try:
    from src.extraction.ingestion_error import IngestionError
    from src.extraction.connection import connect_to_db
    from src.extraction.logger import logger
except ImportError:
    from ingestion_error import IngestionError
    from connection import connect_to_db
    from logger import logger
import re


def get_new_data_from_database(credentials_id, last_extraction=None):
    """
    this function takes arguments of the name of the database credentials in aws secrets manager
    and the most recent extraction time represented as a string
    or None if there is no previous extraction time recorded

    the origin database is queried for data that has been updated since the last extraction time
    or for all the data if the extraction time is None
    and returns the data as a dictionary with keys representing each table in the origin database
    and values of lists of dictionaries representing lines in that table
    the line dictionaries have keys representing the column names of the table
    """

    logger.info("get_new_data_from_database invoked")

    now = str(datetime.now())

    timeframe_string = ""
    last_extraction_time = None
    if last_extraction:
        last_extraction_time = destring_timestamp(last_extraction_time)
        timeframe_string = (
            f"WHERE last_updated BETWEEN '{last_extraction_time}' and '{now}'"
        )
        # you can just give pycopg2 a python datetime and it makes it work

    conn = None
    try:
        new_data = {"extraction_time": now, "data": {}}

        tables = get_tables(credentials_id)

        conn = connect_to_db(credentials_id)
        cursor = conn.cursor()

        for table in tables:
            if table != "_prisma_migrations":  # this is a table that we dont need

                cursor.execute(
                    f"""
                    SELECT *
                    FROM {table}
                    {timeframe_string}
                """
                )

                logger.info(
                    f"""
                    SELECT *
                    FROM {table}
                    {timeframe_string}
                """
                )

                results = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                new_data["data"][table] = [
                    {column_names[i]: str(result[i]) for i in range(len(column_names))}
                    for result in results
                ]

        return new_data

    except Exception as e:
        raise IngestionError(e)

    finally:
        if conn:
            conn.close()


def get_tables(credentials_id):
    table_query = """
        SELECT * 
        FROM pg_tables
        WHERE schemaname = 'public';
    """
    conn = None
    try:
        conn = connect_to_db(credentials_id)
        cursor = conn.cursor()
        cursor.execute(table_query)
        table_query_results = cursor.fetchall()
        tables = [table[1] for table in table_query_results]

        return tables

    except Exception as e:

        raise IngestionError(f"get_tables: {e}")

    finally:
        if conn:
            conn.close()


def destring_timestamp(datetime_string):
    # date time string is of the form 2024-11-13 16:14:04.060884
    datetime_split = re.findall("[0-9]+", datetime_string)
    if len(datetime_split) == 7:
        return datetime(
            int(datetime_split[0]),
            int(datetime_split[1]),
            int(datetime_split[2]),
            int(datetime_split[3]),
            int(datetime_split[4]),
            int(datetime_split[5]),
            int(datetime_split[6]),
        )
    elif len(datetime_split) == 6:
        return datetime(
            int(datetime_split[0]),
            int(datetime_split[1]),
            int(datetime_split[2]),
            int(datetime_split[3]),
            int(datetime_split[4]),
            int(datetime_split[5]),
        )
    else: 
        raise Exception("datetime error")