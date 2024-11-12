from datetime import datetime
from connection import connect_to_db
from pprint import pprint


def get_new_data_from_database(credentials_id, last_update=None):
    now = datetime.now()
    timeframe_string = ""
    if last_update:
        timeframe_string = f"WHERE last_updated BETWEEN {last_update} AND {now}"

    updated_data = {now: []}
    tables = get_tables(credentials_id)
    print(f"there are {len(tables)} and they are {tables}")
    for table in tables:
        if table != "_prisma_migrations":
            cursor = connect_to_db(credentials_id)
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


def get_tables(credentials_id):
    table_query = """
        SELECT * 
        FROM pg_tables
        WHERE schemaname = 'public';
    """
    cursor = connect_to_db(credentials_id)
    cursor.execute(table_query)
    table_query_results = cursor.fetchall()
    tables = [table[1] for table in table_query_results]
    return tables


if __name__ == '__main__':
    pprint(get_new_data_from_database("totesys-db-creds"))