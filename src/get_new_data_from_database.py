from datetime import datetime
from connection import connect_to_db


def get_new_data_from_database(database_name, last_update=None):
    now = datetime.now()
    timeframe_string = ""
    if last_update:
        timeframe_string = f"WHERE last_updated BETWEEN {last_update} AND {now}"

    updated_data = {now: []}
    tables = get_tables(database_name)
    for table in tables:
        cursor = connect_to_db(database_name)
        cursor.execute(
            f"""
            SELECT *
            FROM {table}
            {timeframe_string}
        """
        )
        results = cursor.fetchall()
        updated_data[now].append(results)

    return updated_data


def get_tables(database_name):
    table_query = """
        SELECT * 
        FROM pg_tables
        WHERE schemaname = 'public';
    """
    cursor = connect_to_db(database_name)
    cursor.execute(table_query)
    table_query_results = cursor.fetchall()
    tables = [table[1] for table in table_query_results]
    return tables

print(get_new_data_from_database())