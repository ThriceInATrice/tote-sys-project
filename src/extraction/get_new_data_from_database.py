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
    logger.info("get_new_data_from_database invoked")

    now = str(datetime.now())

    timeframe_string = ""
    if last_extraction:
        last_extraction_time = destring_timestamp(last_extraction)
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
                    make_dict(column_names, result) for result in results
                ]

        return new_data

    except Exception as e:
        raise IngestionError(e)

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


def make_dict(column_names, values):
    if len(column_names) == len(values):
        return {column_names[i]: str(values[i]) for i in range(len(column_names))}
    else:
        raise Exception("data_error")


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


