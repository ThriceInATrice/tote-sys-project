from datetime import datetime
from connection import connect_to_db



def get_new_data_from_database(credentials_id, last_update=None):
    now = datetime.now()
    timeframe_string = ""
    if last_update:
        timeframe_string = f"WHERE last_updated BETWEEN {last_update} AND {now}"
    try:
        updated_data = {now: []}
        tables = get_tables(credentials_id)
        for table in tables:
            if table != "_prisma_migrations":
                cursor = connect_to_db(credentials_id)
                cursor.execute(
                    f"""
                    SELECT *
                    FROM {table}
                    {timeframe_string}
                    LIMIT 1
                """
                )
                results = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                updated_data[now].append([make_dict(column_names, result) for result in results])

        return updated_data
    except Exception as e:
        print("Database connection failed due to {}".format(e))

def make_dict(column_names, values):
    if len(column_names) == len(values):
        return {column_names[i]: values[i] for i in range(len(column_names))}
    else:
        raise Exception("data_error")
    
def get_tables(credentials_id):
    table_query = """
        SELECT * 
        FROM pg_tables
        WHERE schemaname = 'public';
    """
    try:
        cursor = connect_to_db(credentials_id)
        cursor.execute(table_query)
        table_query_results = cursor.fetchall()
        tables = [table[1] for table in table_query_results]
        return tables

    except Exception as e:
        print("Database connection failed due to {}".format(e))

if __name__ == "__main__":
    print(get_new_data_from_database("totesys-db-creds"))
